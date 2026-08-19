use crate::{
    CheckLevel, DEFAULT_API_PORT, DEFAULT_WEB_PORT, DeploymentStatus, DoctorReport,
    GitHubAccessSubject, GitHubInstanceConfig, IngressMode, Instance, PendingGitHubApp,
    PluginConnectOptions, PluginEnvironmentInput, RuntimeSource, SetupError,
    suggest_available_port,
};
use crossterm::{
    event::{self, Event, KeyCode, KeyEvent, KeyModifiers},
    execute,
    terminal::{EnterAlternateScreen, LeaveAlternateScreen, disable_raw_mode, enable_raw_mode},
};
use donkeyspace_github::GitHubRepository;
use ratatui::{
    Frame, Terminal,
    backend::CrosstermBackend,
    layout::{Alignment, Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, ListState, Paragraph, Wrap},
};
use std::{
    collections::BTreeMap,
    io::{self, IsTerminal, Stdout},
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

const HOME_ACTIONS: &[&str] = &[
    "Run doctor",
    "Start Donkeyspace",
    "Stop Donkeyspace",
    "Configure ports",
    "Configure GitHub",
    "Manage GitHub access",
    "Configure Codex",
    "Manage plugins",
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Screen {
    Init,
    Home,
    Ports,
    GitHubMethod,
    GitHubManifest,
    GitHubInstall,
    GitHubExisting,
    GitHubPat,
    RepositorySelect,
    GitHubAccessRepositories,
    GitHubAccessSubjects,
    GitHubAccessType,
    GitHubAccessForm,
    CodexMethod,
    CodexApiKey,
    Doctor,
    Plugins,
    PluginConnect,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GitHubFlow {
    Pending,
    Existing,
    Pat,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GitHubAccessKind {
    User,
    Organization,
    Team,
}

enum TaskResult {
    Status(Result<DeploymentStatus, SetupError>),
    Doctor(Result<DoctorReport, SetupError>),
    Start(Result<(), SetupError>),
    Stop(Result<(), SetupError>),
    Manifest(Result<PendingGitHubApp, SetupError>),
    Repositories {
        flow: GitHubFlow,
        installation_id: Option<u64>,
        result: Result<Vec<GitHubRepository>, SetupError>,
    },
    SaveGitHub(Result<(), SetupError>),
    Access(Result<String, SetupError>),
    Codex(Result<(), SetupError>),
    Plugin(Result<String, SetupError>),
}

struct App {
    config_dir: Option<PathBuf>,
    screen: Screen,
    selected: usize,
    fields: Vec<String>,
    field: usize,
    webhook: bool,
    busy: Option<String>,
    notice: Option<String>,
    error: Option<String>,
    status: DeploymentStatus,
    doctor: Option<DoctorReport>,
    repositories: Vec<GitHubRepository>,
    repository_selected: Vec<bool>,
    repository_cursor: usize,
    github_flow: GitHubFlow,
    pending: Option<PendingGitHubApp>,
    installation_id: Option<u64>,
    access_repository: Option<String>,
    access_kind: GitHubAccessKind,
    confirm_remove: bool,
    continue_setup_after_access: bool,
    pat: Option<String>,
    should_quit: bool,
    last_refresh: Instant,
}

impl App {
    fn new(instance: &Instance, config_dir: Option<PathBuf>) -> Result<Self, SetupError> {
        let pending = instance.pending_github_app()?;
        let initialized = instance.is_initialized();
        let screen = if !instance.is_initialized() {
            Screen::Init
        } else if pending.is_some() {
            Screen::GitHubInstall
        } else {
            Screen::Home
        };
        let (fields, port_notice) = if initialized {
            (vec![String::new()], None)
        } else {
            let api_port =
                suggest_available_port(DEFAULT_API_PORT, &[]).unwrap_or(DEFAULT_API_PORT);
            let web_port =
                suggest_available_port(DEFAULT_WEB_PORT, &[api_port]).unwrap_or(DEFAULT_WEB_PORT);
            let notice = (api_port != DEFAULT_API_PORT || web_port != DEFAULT_WEB_PORT).then(|| {
                format!(
                    "Default ports were busy; suggested API {api_port} and dashboard {web_port}."
                )
            });
            (
                vec![String::new(), api_port.to_string(), web_port.to_string()],
                notice,
            )
        };
        Ok(Self {
            config_dir,
            screen,
            selected: 0,
            fields,
            field: 0,
            webhook: false,
            busy: None,
            notice: pending
                .as_ref()
                .map(|pending| format!("Resume installation of GitHub App `{}`.", pending.slug))
                .or(port_notice),
            error: None,
            status: DeploymentStatus { services: vec![] },
            doctor: None,
            repositories: vec![],
            repository_selected: vec![],
            repository_cursor: 0,
            github_flow: GitHubFlow::Pending,
            pending,
            installation_id: None,
            access_repository: None,
            access_kind: GitHubAccessKind::User,
            confirm_remove: false,
            continue_setup_after_access: false,
            pat: None,
            should_quit: false,
            last_refresh: Instant::now() - Duration::from_secs(3),
        })
    }

    fn reset_messages(&mut self) {
        self.error = None;
        self.notice = None;
    }

    fn show_home(&mut self) {
        self.screen = Screen::Home;
        self.selected = 0;
        self.fields = vec![String::new()];
        self.field = 0;
        self.busy = None;
    }

    fn begin_github(&mut self) {
        self.screen = Screen::GitHubMethod;
        self.selected = 0;
        self.reset_messages();
    }

    fn begin_codex(&mut self) {
        self.screen = Screen::CodexMethod;
        self.selected = 0;
        self.reset_messages();
    }

    fn begin_github_access(&mut self, instance: &Instance) {
        if configured_github_repositories(instance).is_empty() {
            self.error = Some("Configure GitHub repositories first.".into());
            return;
        }
        self.screen = Screen::GitHubAccessRepositories;
        self.selected = 0;
        self.access_repository = None;
        self.confirm_remove = false;
        self.continue_setup_after_access = false;
        self.reset_messages();
    }

    fn begin_plugins(&mut self) {
        self.screen = Screen::Plugins;
        self.selected = 0;
        self.reset_messages();
    }

    fn begin_ports(&mut self, instance: &Instance) {
        let Some(config) = instance.config() else {
            self.error = Some("Instance is not initialized.".into());
            return;
        };
        self.screen = Screen::Ports;
        self.fields = vec![config.api_port.to_string(), config.web_port.to_string()];
        self.field = 0;
        self.reset_messages();
    }

    fn polling_warning(&self, instance: &Instance) -> bool {
        instance
            .config()
            .and_then(|config| config.github.as_ref())
            .is_some_and(|github| {
                matches!(
                    github,
                    GitHubInstanceConfig::App {
                        ingress: IngressMode::Polling,
                        ..
                    } | GitHubInstanceConfig::Pat {
                        ingress: IngressMode::Polling,
                        ..
                    }
                )
            })
    }
}

struct TerminalGuard {
    terminal: Terminal<CrosstermBackend<Stdout>>,
    active: bool,
}

impl TerminalGuard {
    fn enter() -> Result<Self, SetupError> {
        enable_raw_mode()?;
        let mut stdout = io::stdout();
        if let Err(error) = execute!(stdout, EnterAlternateScreen) {
            let _ = disable_raw_mode();
            return Err(error.into());
        }
        let terminal = Terminal::new(CrosstermBackend::new(stdout))?;
        Ok(Self {
            terminal,
            active: true,
        })
    }

    fn suspend(&mut self) -> Result<(), SetupError> {
        if self.active {
            disable_raw_mode()?;
            execute!(self.terminal.backend_mut(), LeaveAlternateScreen)?;
            self.terminal.show_cursor()?;
            self.active = false;
        }
        Ok(())
    }

    fn resume(&mut self) -> Result<(), SetupError> {
        if !self.active {
            enable_raw_mode()?;
            execute!(self.terminal.backend_mut(), EnterAlternateScreen)?;
            self.terminal.clear()?;
            self.active = true;
        }
        Ok(())
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        if self.active {
            let _ = disable_raw_mode();
            let _ = execute!(self.terminal.backend_mut(), LeaveAlternateScreen);
            let _ = self.terminal.show_cursor();
        }
    }
}

pub async fn run(config_dir: Option<PathBuf>) -> Result<(), SetupError> {
    if !io::stdin().is_terminal() || !io::stdout().is_terminal() {
        return Err(SetupError::Config(
            "interactive TUI requires a terminal; use an explicit donkeyspace subcommand for automation"
                .into(),
        ));
    }
    let instance = Instance::open(config_dir.clone())?;
    let mut app = App::new(&instance, config_dir)?;
    let mut terminal = TerminalGuard::enter()?;
    let (sender, mut receiver) = mpsc::unbounded_channel();

    loop {
        let current = Instance::open(app.config_dir.clone())?;
        terminal
            .terminal
            .draw(|frame| render(frame, &app, &current))?;
        if app.should_quit {
            break;
        }

        while let Ok(result) = receiver.try_recv() {
            apply_task_result(&mut app, result);
        }
        if app.screen == Screen::Doctor && app.doctor.is_none() && app.busy.is_none() {
            spawn_doctor(&mut app, sender.clone());
        }
        if app.screen == Screen::Home
            && app.busy.is_none()
            && app.last_refresh.elapsed() >= Duration::from_secs(2)
        {
            app.last_refresh = Instant::now();
            spawn_status(app.config_dir.clone(), sender.clone());
        }
        if event::poll(Duration::from_millis(100))?
            && let Event::Key(key) = event::read()?
        {
            handle_key(&mut app, &mut terminal, key, sender.clone()).await?;
        }
    }
    Ok(())
}

fn spawn_status(config_dir: Option<PathBuf>, sender: mpsc::UnboundedSender<TaskResult>) {
    tokio::task::spawn_blocking(move || {
        let result = Instance::open(config_dir).and_then(|instance| instance.deployment_status());
        let _ = sender.send(TaskResult::Status(result));
    });
}

fn spawn_operation(
    config_dir: Option<PathBuf>,
    sender: mpsc::UnboundedSender<TaskResult>,
    operation: &'static str,
) {
    tokio::task::spawn_blocking(move || {
        let result = Instance::open(config_dir).and_then(|instance| match operation {
            "start" => instance.start(),
            "stop" => instance.stop(),
            _ => unreachable!(),
        });
        let message = if operation == "start" {
            TaskResult::Start(result)
        } else {
            TaskResult::Stop(result)
        };
        let _ = sender.send(message);
    });
}

fn apply_task_result(app: &mut App, result: TaskResult) {
    app.busy = None;
    match result {
        TaskResult::Status(Ok(status)) => app.status = status,
        TaskResult::Status(Err(error)) => app.error = Some(error.to_string()),
        TaskResult::Doctor(Ok(report)) => {
            app.doctor = Some(report);
            app.screen = Screen::Doctor;
        }
        TaskResult::Doctor(Err(error))
        | TaskResult::Start(Err(error))
        | TaskResult::Stop(Err(error))
        | TaskResult::Manifest(Err(error))
        | TaskResult::SaveGitHub(Err(error))
        | TaskResult::Access(Err(error))
        | TaskResult::Codex(Err(error))
        | TaskResult::Plugin(Err(error)) => app.error = Some(error.to_string()),
        TaskResult::Start(Ok(())) => app.notice = Some("Donkeyspace started.".into()),
        TaskResult::Stop(Ok(())) => {
            app.status.services.clear();
            app.notice = Some("Donkeyspace stopped; volumes were preserved.".into());
        }
        TaskResult::Manifest(Ok(pending)) => {
            app.pending = Some(pending);
            app.screen = Screen::GitHubInstall;
            app.notice = Some("Install the App in the browser, then press Enter.".into());
        }
        TaskResult::Repositories {
            flow,
            installation_id,
            result: Ok(repositories),
        } => {
            app.github_flow = flow;
            app.installation_id = installation_id;
            app.repository_selected = vec![false; repositories.len()];
            app.repositories = repositories;
            app.repository_cursor = 0;
            app.screen = Screen::RepositorySelect;
            if app.repositories.is_empty() {
                app.error = Some("No accessible repositories were returned.".into());
            }
        }
        TaskResult::Repositories {
            result: Err(error), ..
        } => app.error = Some(error.to_string()),
        TaskResult::SaveGitHub(Ok(())) => {
            app.notice = Some("GitHub connection saved and validated.".into());
            let codex_connected = Instance::open(app.config_dir.clone())
                .ok()
                .and_then(|instance| instance.config().cloned())
                .and_then(|config| config.codex_home)
                .is_some();
            match Instance::open(app.config_dir.clone()) {
                Ok(instance) => {
                    app.begin_github_access(&instance);
                    app.continue_setup_after_access = !codex_connected;
                    app.notice = Some(
                        "GitHub connected. Configure trusted identities before continuing.".into(),
                    );
                }
                Err(error) => app.error = Some(error.to_string()),
            }
        }
        TaskResult::Access(Ok(message)) => {
            app.notice = Some(message);
            app.screen = Screen::GitHubAccessSubjects;
            app.selected = 0;
            app.confirm_remove = false;
        }
        TaskResult::Codex(Ok(())) => {
            app.notice = Some("Codex authentication verified.".into());
            app.screen = Screen::Doctor;
            app.doctor = None;
        }
        TaskResult::Plugin(Ok(message)) => {
            app.notice = Some(message);
            app.screen = Screen::Plugins;
            app.selected = 0;
        }
    }
}

async fn handle_key(
    app: &mut App,
    terminal: &mut TerminalGuard,
    key: KeyEvent,
    sender: mpsc::UnboundedSender<TaskResult>,
) -> Result<(), SetupError> {
    if key.modifiers.contains(KeyModifiers::CONTROL) && key.code == KeyCode::Char('c') {
        app.should_quit = true;
        return Ok(());
    }
    if app.busy.is_some() {
        return Ok(());
    }
    app.error = None;
    match app.screen {
        Screen::Init => handle_init(app, key)?,
        Screen::Home => handle_home(app, key, sender),
        Screen::Ports => handle_ports(app, key)?,
        Screen::GitHubMethod => handle_github_method(app, key),
        Screen::GitHubManifest => handle_manifest_form(app, key, sender),
        Screen::GitHubInstall => handle_install(app, key, sender),
        Screen::GitHubExisting => handle_existing(app, key, sender),
        Screen::GitHubPat => handle_pat(app, key, sender),
        Screen::RepositorySelect => handle_repositories(app, key, sender),
        Screen::GitHubAccessRepositories => handle_access_repositories(app, key)?,
        Screen::GitHubAccessSubjects => handle_access_subjects(app, key, sender)?,
        Screen::GitHubAccessType => handle_access_type(app, key)?,
        Screen::GitHubAccessForm => handle_access_form(app, key, sender)?,
        Screen::CodexMethod => handle_codex_method(app, terminal, key, sender)?,
        Screen::CodexApiKey => handle_api_key(app, key, sender),
        Screen::Doctor => handle_doctor(app, key, sender),
        Screen::Plugins => handle_plugins(app, key, sender)?,
        Screen::PluginConnect => handle_plugin_connect(app, key, sender),
    }
    Ok(())
}

fn handle_init(app: &mut App, key: KeyEvent) -> Result<(), SetupError> {
    match key.code {
        KeyCode::Tab | KeyCode::Down => app.field = (app.field + 1) % 3,
        KeyCode::BackTab | KeyCode::Up => app.field = (app.field + 2) % 3,
        KeyCode::Enter => {
            let source = if app.fields[0].trim().is_empty() {
                PathBuf::from(".")
            } else {
                PathBuf::from(app.fields[0].trim())
            };
            let api_port = match parse_port(&app.fields[1], "API port") {
                Ok(port) => port,
                Err(error) => {
                    app.error = Some(error);
                    return Ok(());
                }
            };
            let web_port = match parse_port(&app.fields[2], "Dashboard port") {
                Ok(port) => port,
                Err(error) => {
                    app.error = Some(error);
                    return Ok(());
                }
            };
            let mut instance = Instance::open(app.config_dir.clone())?;
            if let Err(error) = instance.init_with_ports(
                source,
                RuntimeSource::LocalBuild,
                Some(api_port),
                Some(web_port),
            ) {
                app.error = Some(error.to_string());
                return Ok(());
            }
            app.notice = Some("Instance initialized with local-build runtime.".into());
            app.begin_github();
        }
        KeyCode::Esc => app.should_quit = true,
        _ => edit_field(&mut app.fields[app.field], key, false),
    }
    Ok(())
}

fn handle_ports(app: &mut App, key: KeyEvent) -> Result<(), SetupError> {
    match key.code {
        KeyCode::Esc => app.show_home(),
        KeyCode::Tab | KeyCode::Down => app.field = (app.field + 1) % 2,
        KeyCode::BackTab | KeyCode::Up => app.field = (app.field + 1) % 2,
        KeyCode::Enter => {
            let api_port = match parse_port(&app.fields[0], "API port") {
                Ok(port) => port,
                Err(error) => {
                    app.error = Some(error);
                    return Ok(());
                }
            };
            let web_port = match parse_port(&app.fields[1], "Dashboard port") {
                Ok(port) => port,
                Err(error) => {
                    app.error = Some(error);
                    return Ok(());
                }
            };
            let stack_running = app
                .status
                .services
                .iter()
                .any(|service| service.state.eq_ignore_ascii_case("running"));
            let mut instance = Instance::open(app.config_dir.clone())?;
            if let Err(error) = instance.configure_ports(Some(api_port), Some(web_port)) {
                app.error = Some(error.to_string());
                return Ok(());
            }
            app.show_home();
            app.notice = Some(if stack_running {
                "Ports saved. Stop and start Donkeyspace to apply them.".into()
            } else {
                "Ports saved.".into()
            });
        }
        _ => edit_field(&mut app.fields[app.field], key, false),
    }
    Ok(())
}

fn parse_port(value: &str, label: &str) -> Result<u16, String> {
    value
        .trim()
        .parse::<u16>()
        .ok()
        .filter(|port| *port > 0)
        .ok_or_else(|| format!("{label} must be between 1 and 65535."))
}

fn handle_home(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    match key.code {
        KeyCode::Char('q') | KeyCode::Esc => app.should_quit = true,
        KeyCode::Up => app.selected = app.selected.saturating_sub(1),
        KeyCode::Down => app.selected = (app.selected + 1).min(HOME_ACTIONS.len() - 1),
        KeyCode::Enter => match app.selected {
            0 => {
                app.busy = Some("Running diagnostics…".into());
                let config_dir = app.config_dir.clone();
                tokio::spawn(async move {
                    let result = match Instance::open(config_dir) {
                        Ok(instance) => instance.doctor_report().await,
                        Err(error) => Err(error),
                    };
                    let _ = sender.send(TaskResult::Doctor(result));
                });
            }
            1 => {
                app.busy = Some("Building and starting services…".into());
                spawn_operation(app.config_dir.clone(), sender, "start");
            }
            2 => {
                app.busy = Some("Stopping services…".into());
                spawn_operation(app.config_dir.clone(), sender, "stop");
            }
            3 => match Instance::open(app.config_dir.clone()) {
                Ok(instance) => app.begin_ports(&instance),
                Err(error) => app.error = Some(error.to_string()),
            },
            4 => app.begin_github(),
            5 => match Instance::open(app.config_dir.clone()) {
                Ok(instance) => app.begin_github_access(&instance),
                Err(error) => app.error = Some(error.to_string()),
            },
            6 => app.begin_codex(),
            7 => app.begin_plugins(),
            _ => {}
        },
        _ => {}
    }
}

fn configured_github_repositories(instance: &Instance) -> Vec<String> {
    match instance.config().and_then(|config| config.github.as_ref()) {
        Some(GitHubInstanceConfig::App { repositories, .. })
        | Some(GitHubInstanceConfig::Pat { repositories, .. }) => repositories.clone(),
        None => Vec::new(),
    }
}

fn handle_access_repositories(app: &mut App, key: KeyEvent) -> Result<(), SetupError> {
    let instance = Instance::open(app.config_dir.clone())?;
    let repositories = configured_github_repositories(&instance);
    match key.code {
        KeyCode::Esc | KeyCode::Char('b') => {
            if app.continue_setup_after_access {
                app.continue_setup_after_access = false;
                app.begin_codex();
            } else {
                app.show_home();
            }
        }
        KeyCode::Up => app.selected = app.selected.saturating_sub(1),
        KeyCode::Down if !repositories.is_empty() => {
            app.selected = (app.selected + 1).min(repositories.len() - 1)
        }
        KeyCode::Enter if !repositories.is_empty() => {
            app.access_repository = Some(repositories[app.selected].clone());
            app.screen = Screen::GitHubAccessSubjects;
            app.selected = 0;
            app.confirm_remove = false;
            app.reset_messages();
        }
        _ => {}
    }
    Ok(())
}

fn handle_access_subjects(
    app: &mut App,
    key: KeyEvent,
    sender: mpsc::UnboundedSender<TaskResult>,
) -> Result<(), SetupError> {
    let repository = app
        .access_repository
        .clone()
        .ok_or_else(|| SetupError::Config("access repository is missing".into()))?;
    let instance = Instance::open(app.config_dir.clone())?;
    let subjects = instance.github_access(&repository)?.to_vec();
    match key.code {
        KeyCode::Esc | KeyCode::Char('b') => {
            app.screen = Screen::GitHubAccessRepositories;
            app.selected = 0;
            app.confirm_remove = false;
        }
        KeyCode::Up => {
            app.selected = app.selected.saturating_sub(1);
            app.confirm_remove = false;
        }
        KeyCode::Down if !subjects.is_empty() => {
            app.selected = (app.selected + 1).min(subjects.len() - 1);
            app.confirm_remove = false;
        }
        KeyCode::Char('a') => {
            app.screen = Screen::GitHubAccessType;
            app.selected = 0;
            app.confirm_remove = false;
        }
        KeyCode::Char('d') if !subjects.is_empty() => {
            if subjects.len() == 1 && !app.confirm_remove {
                app.confirm_remove = true;
                app.notice = Some(
                    "This is the final trusted subject; press d again to deny all engagement."
                        .into(),
                );
                return Ok(());
            }
            let subject = subjects[app.selected].clone();
            app.busy = Some(format!("Removing {}…", subject.display_name()));
            let config_dir = app.config_dir.clone();
            tokio::task::spawn_blocking(move || {
                let result = Instance::open(config_dir).and_then(|mut instance| {
                    instance.remove_github_access(&repository, &subject)?;
                    Ok(format!(
                        "Removed {} from {repository}.",
                        subject.display_name()
                    ))
                });
                let _ = sender.send(TaskResult::Access(result));
            });
        }
        _ => app.confirm_remove = false,
    }
    Ok(())
}

fn handle_access_type(app: &mut App, key: KeyEvent) -> Result<(), SetupError> {
    match key.code {
        KeyCode::Esc => app.screen = Screen::GitHubAccessSubjects,
        KeyCode::Up => app.selected = app.selected.saturating_sub(1),
        KeyCode::Down => app.selected = (app.selected + 1).min(3),
        KeyCode::Enter => {
            if app.selected == 3 {
                app.screen = Screen::GitHubAccessSubjects;
                app.selected = 0;
                return Ok(());
            }
            let repository = app
                .access_repository
                .as_deref()
                .ok_or_else(|| SetupError::Config("access repository is missing".into()))?;
            let owner = repository.split_once('/').expect("configured repository").0;
            app.access_kind = match app.selected {
                0 => GitHubAccessKind::User,
                1 => GitHubAccessKind::Organization,
                2 => GitHubAccessKind::Team,
                _ => unreachable!(),
            };
            app.fields = match app.access_kind {
                GitHubAccessKind::User => vec![String::new()],
                GitHubAccessKind::Organization => vec![owner.to_string()],
                GitHubAccessKind::Team => vec![owner.to_string(), String::new()],
            };
            app.field = 0;
            app.screen = Screen::GitHubAccessForm;
        }
        _ => {}
    }
    Ok(())
}

fn handle_access_form(
    app: &mut App,
    key: KeyEvent,
    sender: mpsc::UnboundedSender<TaskResult>,
) -> Result<(), SetupError> {
    let field_count = app.fields.len();
    match key.code {
        KeyCode::Esc => {
            app.screen = Screen::GitHubAccessType;
            app.selected = 0;
        }
        KeyCode::Tab | KeyCode::Down => app.field = (app.field + 1) % field_count,
        KeyCode::BackTab | KeyCode::Up => app.field = (app.field + field_count - 1) % field_count,
        KeyCode::Enter => {
            let subject = match app.access_kind {
                GitHubAccessKind::User => GitHubAccessSubject::User {
                    login: app.fields[0].trim().into(),
                },
                GitHubAccessKind::Organization => GitHubAccessSubject::Organization {
                    login: app.fields[0].trim().into(),
                },
                GitHubAccessKind::Team => GitHubAccessSubject::Team {
                    organization: app.fields[0].trim().into(),
                    team_slug: app.fields[1].trim().into(),
                },
            };
            if subject.display_name().ends_with(':') || subject.display_name().ends_with('/') {
                app.error = Some("GitHub access value cannot be empty.".into());
                return Ok(());
            }
            let repository = app
                .access_repository
                .clone()
                .ok_or_else(|| SetupError::Config("access repository is missing".into()))?;
            app.busy = Some(format!("Validating {}…", subject.display_name()));
            let config_dir = app.config_dir.clone();
            tokio::spawn(async move {
                let result =
                    match Instance::open(config_dir) {
                        Ok(mut instance) => instance
                            .add_github_access(&repository, subject)
                            .await
                            .map(|subject| {
                                format!("Added {} to {repository}.", subject.display_name())
                            }),
                        Err(error) => Err(error),
                    };
                let _ = sender.send(TaskResult::Access(result));
            });
        }
        _ => edit_field(&mut app.fields[app.field], key, false),
    }
    Ok(())
}

fn handle_plugins(
    app: &mut App,
    key: KeyEvent,
    sender: mpsc::UnboundedSender<TaskResult>,
) -> Result<(), SetupError> {
    let instance = Instance::open(app.config_dir.clone())?;
    let flows = plugin_flows(&instance);
    match key.code {
        KeyCode::Esc | KeyCode::Char('b') => app.show_home(),
        KeyCode::Up => app.selected = app.selected.saturating_sub(1),
        KeyCode::Down if !flows.is_empty() => {
            app.selected = (app.selected + 1).min(flows.len() - 1)
        }
        KeyCode::Char('c') => {
            app.screen = Screen::PluginConnect;
            app.fields = vec![String::new(), String::new(), String::new()];
            app.field = 0;
        }
        KeyCode::Char('d') => {
            app.busy = Some("Disabling active plugin flow…".into());
            spawn_plugin_operation(app.config_dir.clone(), sender, |mut instance| {
                instance.disable_plugin()?;
                Ok("Plugin flow disabled; installations were preserved.".into())
            });
        }
        KeyCode::Char('r') if !flows.is_empty() => {
            let id = flows[app.selected].0.clone();
            app.busy = Some(format!("Rebuilding {id}…"));
            spawn_plugin_operation(app.config_dir.clone(), sender, move |instance| {
                instance.rebuild_plugin(&id)?;
                Ok(format!("Rebuilt {id}."))
            });
        }
        KeyCode::Enter if !flows.is_empty() => {
            let (id, flow, _) = flows[app.selected].clone();
            app.busy = Some(format!("Activating {id}:{flow}…"));
            spawn_plugin_operation(app.config_dir.clone(), sender, move |mut instance| {
                instance.activate_plugin(&id, &flow)?;
                Ok(format!("Activated {id}:{flow}."))
            });
        }
        _ => {}
    }
    Ok(())
}

fn handle_plugin_connect(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    match key.code {
        KeyCode::Esc => app.begin_plugins(),
        KeyCode::Tab | KeyCode::Down => app.field = (app.field + 1) % 3,
        KeyCode::BackTab | KeyCode::Up => app.field = (app.field + 2) % 3,
        KeyCode::Enter => {
            let path = PathBuf::from(app.fields[0].trim());
            if path.as_os_str().is_empty() {
                app.error = Some("Plugin path is required.".into());
                return;
            }
            let flow = (!app.fields[1].trim().is_empty()).then(|| app.fields[1].trim().to_string());
            let environment = match parse_plugin_environment_files(&app.fields[2]) {
                Ok(environment) => environment,
                Err(error) => {
                    app.error = Some(error);
                    return;
                }
            };
            app.busy = Some("Validating and building plugin…".into());
            spawn_plugin_operation(app.config_dir.clone(), sender, move |mut instance| {
                instance.connect_plugin(PluginConnectOptions {
                    path,
                    flow,
                    environment,
                })?;
                Ok("Plugin connected. Press Enter on a flow to activate it.".into())
            });
        }
        _ => edit_field(&mut app.fields[app.field], key, false),
    }
}

fn parse_plugin_environment_files(
    value: &str,
) -> Result<BTreeMap<String, PluginEnvironmentInput>, String> {
    let mut result = BTreeMap::new();
    for item in value
        .split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
    {
        let (name, path) = item.split_once('=').ok_or_else(|| {
            "Environment files must use NAME=PATH, separated by commas.".to_string()
        })?;
        if name.is_empty() || path.is_empty() {
            return Err("Environment files must use NAME=PATH, separated by commas.".into());
        }
        result.insert(
            name.to_string(),
            PluginEnvironmentInput::File(PathBuf::from(path)),
        );
    }
    Ok(result)
}

fn spawn_plugin_operation<F>(
    config_dir: Option<PathBuf>,
    sender: mpsc::UnboundedSender<TaskResult>,
    operation: F,
) where
    F: FnOnce(Instance) -> Result<String, SetupError> + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let result = Instance::open(config_dir).and_then(operation);
        let _ = sender.send(TaskResult::Plugin(result));
    });
}

fn plugin_flows(instance: &Instance) -> Vec<(String, String, crate::PluginFlowClass)> {
    instance
        .config()
        .into_iter()
        .flat_map(|config| &config.plugins)
        .flat_map(|(id, plugin)| {
            plugin
                .flows
                .iter()
                .map(move |(flow, class)| (id.clone(), flow.clone(), *class))
        })
        .collect()
}

fn handle_github_method(app: &mut App, key: KeyEvent) {
    let options = 4;
    match key.code {
        KeyCode::Up => app.selected = app.selected.saturating_sub(1),
        KeyCode::Down => app.selected = (app.selected + 1).min(options - 1),
        KeyCode::Esc => app.show_home(),
        KeyCode::Enter => match app.selected {
            0 => {
                app.screen = Screen::GitHubManifest;
                app.fields = vec![String::new(), String::new(), String::new()];
                app.field = 0;
                app.webhook = false;
            }
            1 => {
                app.screen = Screen::GitHubExisting;
                app.fields = vec![
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                    String::new(),
                ];
                app.field = 0;
                app.webhook = false;
            }
            2 => {
                app.screen = Screen::GitHubPat;
                app.fields = vec![String::new(), String::new()];
                app.field = 0;
                app.webhook = false;
            }
            _ => app.show_home(),
        },
        _ => {}
    }
}

fn handle_manifest_form(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    match key.code {
        KeyCode::Esc => app.begin_github(),
        KeyCode::Tab | KeyCode::Down => app.field = (app.field + 1) % 4,
        KeyCode::BackTab | KeyCode::Up => app.field = (app.field + 3) % 4,
        KeyCode::Char(' ') if app.field == 3 => app.webhook = !app.webhook,
        KeyCode::Enter => {
            if app.fields[0].trim().is_empty() {
                app.error = Some("Enter the personal account or organization owner.".into());
                return;
            }
            let owner = app.fields[0].trim().to_string();
            let organization =
                (!app.fields[1].trim().is_empty()).then(|| app.fields[1].trim().to_string());
            let ingress = ingress(app.webhook, app.fields.get(2));
            if app.webhook && app.fields[2].trim().is_empty() {
                app.error = Some("Enter a public HTTPS URL for webhook ingress.".into());
                return;
            }
            app.busy = Some(
                "Waiting for GitHub App registration…\n\nHeadless: on your workstation run\nssh -N -L 8787:127.0.0.1:8787 USER@HOST\nthen open http://127.0.0.1:8787/ in its browser."
                    .into(),
            );
            let config_dir = app.config_dir.clone();
            tokio::task::spawn_blocking(move || {
                let result = Instance::open(config_dir).and_then(|instance| {
                    instance.begin_github_app(owner, ingress, 8787, organization)
                });
                let _ = sender.send(TaskResult::Manifest(result));
            });
        }
        _ if app.field < 3 => edit_field(&mut app.fields[app.field], key, false),
        _ => {}
    }
}

fn handle_install(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    match key.code {
        KeyCode::Esc => app.begin_github(),
        KeyCode::Char('d') => {
            if let Ok(instance) = Instance::open(app.config_dir.clone()) {
                if let Err(error) = instance.discard_pending_github_app() {
                    app.error = Some(error.to_string());
                } else {
                    app.pending = None;
                    app.begin_github();
                }
            }
        }
        KeyCode::Enter => {
            let Some(pending) = app.pending.clone() else {
                app.error = Some("Pending App registration is unavailable.".into());
                return;
            };
            app.busy = Some("Discovering installation repositories…".into());
            let config_dir = app.config_dir.clone();
            tokio::spawn(async move {
                let result = match Instance::open(config_dir) {
                    Ok(instance) => instance.pending_github_repositories(&pending).await,
                    Err(error) => Err(error),
                };
                let message = match result {
                    Ok((installation_id, repositories)) => TaskResult::Repositories {
                        flow: GitHubFlow::Pending,
                        installation_id: Some(installation_id),
                        result: Ok(repositories),
                    },
                    Err(error) => TaskResult::Repositories {
                        flow: GitHubFlow::Pending,
                        installation_id: None,
                        result: Err(error),
                    },
                };
                let _ = sender.send(message);
            });
        }
        _ => {}
    }
}

fn handle_existing(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    handle_advanced_form(app, key, 6, |app| {
        let app_id = parse_id(&app.fields[0], "App ID")?;
        let installation_id = parse_id(&app.fields[1], "installation ID")?;
        let private_key = PathBuf::from(app.fields[2].trim());
        if private_key.as_os_str().is_empty() {
            return Err("Private-key path is required.".into());
        }
        if app.webhook && (app.fields[3].trim().is_empty() || app.fields[4].trim().is_empty()) {
            return Err("Webhook secret file and public HTTPS URL are required.".into());
        }
        app.installation_id = Some(installation_id);
        app.github_flow = GitHubFlow::Existing;
        app.busy = Some("Loading accessible repositories…".into());
        let config_dir = app.config_dir.clone();
        tokio::spawn(async move {
            let result = match Instance::open(config_dir) {
                Ok(instance) => {
                    instance
                        .app_repositories(app_id, installation_id, private_key)
                        .await
                }
                Err(error) => Err(error),
            };
            let _ = sender.send(TaskResult::Repositories {
                flow: GitHubFlow::Existing,
                installation_id: Some(installation_id),
                result,
            });
        });
        Ok(())
    });
}

fn handle_pat(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    handle_advanced_form(app, key, 3, |app| {
        let token = app.fields[0].trim().to_string();
        if token.is_empty() {
            return Err("PAT is required.".into());
        }
        if app.webhook && app.fields[1].trim().is_empty() {
            return Err("Public HTTPS URL is required for webhook ingress.".into());
        }
        app.pat = Some(token.clone());
        app.github_flow = GitHubFlow::Pat;
        app.busy = Some("Loading accessible repositories…".into());
        let config_dir = app.config_dir.clone();
        tokio::spawn(async move {
            let result = match Instance::open(config_dir) {
                Ok(instance) => instance.pat_repositories(&token).await,
                Err(error) => Err(error),
            };
            let _ = sender.send(TaskResult::Repositories {
                flow: GitHubFlow::Pat,
                installation_id: None,
                result,
            });
        });
        Ok(())
    });
}

fn handle_advanced_form(
    app: &mut App,
    key: KeyEvent,
    item_count: usize,
    submit: impl FnOnce(&mut App) -> Result<(), String>,
) {
    match key.code {
        KeyCode::Esc => app.begin_github(),
        KeyCode::Tab | KeyCode::Down => app.field = (app.field + 1) % item_count,
        KeyCode::BackTab | KeyCode::Up => app.field = (app.field + item_count - 1) % item_count,
        KeyCode::Char(' ') if app.field == item_count - 1 => app.webhook = !app.webhook,
        KeyCode::Enter => {
            if let Err(error) = submit(app) {
                app.error = Some(error);
            }
        }
        _ if app.field < app.fields.len() => {
            let secret = app.screen == Screen::GitHubPat && app.field == 0;
            edit_field(&mut app.fields[app.field], key, secret)
        }
        _ => {}
    }
}

fn handle_repositories(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    match key.code {
        KeyCode::Esc => app.begin_github(),
        KeyCode::Up => app.repository_cursor = app.repository_cursor.saturating_sub(1),
        KeyCode::Down => {
            app.repository_cursor =
                (app.repository_cursor + 1).min(app.repositories.len().saturating_sub(1));
        }
        KeyCode::Char(' ') => {
            if let Some(selected) = app.repository_selected.get_mut(app.repository_cursor) {
                *selected = !*selected;
            }
        }
        KeyCode::Enter => {
            let repositories = app
                .repositories
                .iter()
                .zip(&app.repository_selected)
                .filter(|(_, selected)| **selected)
                .map(|(repository, _)| repository.full_name.clone())
                .collect::<Vec<_>>();
            if repositories.is_empty() {
                app.error = Some("Select at least one repository with Space.".into());
                return;
            }
            let owner = repositories[0].split_once('/').unwrap().0.to_string();
            if repositories
                .iter()
                .any(|repository| !repository.starts_with(&format!("{owner}/")))
            {
                app.error = Some("All selected repositories must have one owner.".into());
                return;
            }
            app.busy = Some("Saving and validating GitHub connection…".into());
            let config_dir = app.config_dir.clone();
            let flow = app.github_flow;
            let pending = app.pending.clone();
            let installation_id = app.installation_id;
            let fields = app.fields.clone();
            let webhook = app.webhook;
            let pat = app.pat.take();
            tokio::spawn(async move {
                let result = async {
                    let mut instance = Instance::open(config_dir)?;
                    match flow {
                        GitHubFlow::Pending => {
                            instance
                                .complete_pending_github_app(
                                    pending.ok_or_else(|| {
                                        SetupError::Config("pending App is missing".into())
                                    })?,
                                    installation_id.ok_or_else(|| {
                                        SetupError::Config("installation ID is missing".into())
                                    })?,
                                    repositories,
                                )
                                .await
                        }
                        GitHubFlow::Pat => {
                            instance
                                .connect_github_pat(
                                    pat.as_deref().unwrap_or_default(),
                                    repositories,
                                    ingress(webhook, fields.get(1)),
                                )
                                .await
                        }
                        GitHubFlow::Existing => {
                            let app_id = fields
                                .first()
                                .and_then(|value| value.parse().ok())
                                .ok_or_else(|| SetupError::Config("App ID is invalid".into()))?;
                            let private_key_file =
                                PathBuf::from(fields.get(2).cloned().unwrap_or_default());
                            let webhook_secret_file = fields
                                .get(3)
                                .filter(|value| !value.is_empty())
                                .map(PathBuf::from);
                            instance
                                .connect_github(crate::ConnectGitHubOptions {
                                    app_id: Some(app_id),
                                    installation_id,
                                    private_key_file: Some(private_key_file),
                                    webhook_secret_file,
                                    repositories,
                                    public_url: if webhook {
                                        fields.get(4).cloned().filter(|value| !value.is_empty())
                                    } else {
                                        None
                                    },
                                    callback_port: 8787,
                                    organization: None,
                                    pat: false,
                                })
                                .await
                        }
                    }
                }
                .await;
                let _ = sender.send(TaskResult::SaveGitHub(result));
            });
        }
        _ => {}
    }
}

fn handle_codex_method(
    app: &mut App,
    terminal: &mut TerminalGuard,
    key: KeyEvent,
    sender: mpsc::UnboundedSender<TaskResult>,
) -> Result<(), SetupError> {
    match key.code {
        KeyCode::Up => app.selected = app.selected.saturating_sub(1),
        KeyCode::Down => app.selected = (app.selected + 1).min(2),
        KeyCode::Esc => app.show_home(),
        KeyCode::Enter if app.selected == 0 => {
            terminal.suspend()?;
            let mut instance = Instance::open(app.config_dir.clone())?;
            let result = instance.connect_codex(crate::CodexLoginMethod::ChatGpt);
            terminal.resume()?;
            apply_task_result(app, TaskResult::Codex(result));
            if app.error.is_none() {
                spawn_doctor(app, sender);
            }
        }
        KeyCode::Enter if app.selected == 1 => {
            app.screen = Screen::CodexApiKey;
            app.fields = vec![String::new()];
            app.field = 0;
        }
        KeyCode::Enter => app.show_home(),
        _ => {}
    }
    Ok(())
}

fn handle_api_key(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    match key.code {
        KeyCode::Esc => app.begin_codex(),
        KeyCode::Enter => {
            let key = app.fields[0].clone();
            if key.trim().is_empty() {
                app.error = Some("OpenAI project API key is required.".into());
                return;
            }
            app.fields[0].clear();
            app.busy = Some("Authenticating Codex…".into());
            let config_dir = app.config_dir.clone();
            tokio::task::spawn_blocking(move || {
                let result = Instance::open(config_dir)
                    .and_then(|mut instance| instance.connect_codex_api_key(&key));
                let _ = sender.send(TaskResult::Codex(result));
            });
        }
        _ => edit_field(&mut app.fields[0], key, true),
    }
}

fn spawn_doctor(app: &mut App, sender: mpsc::UnboundedSender<TaskResult>) {
    app.busy = Some("Running diagnostics…".into());
    let config_dir = app.config_dir.clone();
    tokio::spawn(async move {
        let result = match Instance::open(config_dir) {
            Ok(instance) => instance.doctor_report().await,
            Err(error) => Err(error),
        };
        let _ = sender.send(TaskResult::Doctor(result));
    });
}

fn handle_doctor(app: &mut App, key: KeyEvent, sender: mpsc::UnboundedSender<TaskResult>) {
    match key.code {
        KeyCode::Esc | KeyCode::Char('b') => app.show_home(),
        KeyCode::Char('r') => spawn_doctor(app, sender),
        KeyCode::Enter if app.doctor.as_ref().is_some_and(DoctorReport::passed) => {
            app.show_home();
            app.busy = Some("Building and starting services…".into());
            spawn_operation(app.config_dir.clone(), sender, "start");
        }
        _ => {}
    }
}

fn edit_field(value: &mut String, key: KeyEvent, _secret: bool) {
    match key.code {
        KeyCode::Char(character) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            value.push(character)
        }
        KeyCode::Backspace => {
            value.pop();
        }
        _ => {}
    }
}

fn ingress(webhook: bool, public_url: Option<&String>) -> IngressMode {
    if webhook {
        IngressMode::Webhook {
            public_url: public_url.cloned().unwrap_or_default(),
        }
    } else {
        IngressMode::Polling
    }
}

fn parse_id(value: &str, label: &str) -> Result<u64, String> {
    value
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|value| *value > 0)
        .ok_or_else(|| format!("{label} must be a positive integer."))
}

fn render(frame: &mut Frame, app: &App, instance: &Instance) {
    let area = frame.area();
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(area);
    frame.render_widget(
        Paragraph::new(Line::from(vec![
            Span::styled(
                " DONKEYSPACE ",
                Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw("  local installation control plane"),
        ]))
        .block(Block::default().borders(Borders::BOTTOM)),
        vertical[0],
    );
    match app.screen {
        Screen::Init => render_form(
            frame,
            vertical[1],
            "Initialize",
            &["Source tree", "API port", "Dashboard port"],
            &app.fields,
            app.field,
            false,
            "Enter initialize  Esc quit",
        ),
        Screen::Home => render_home(frame, vertical[1], app, instance),
        Screen::Ports => render_form(
            frame,
            vertical[1],
            "Configure host ports",
            &["API port", "Dashboard port"],
            &app.fields,
            app.field,
            false,
            "Enter save  Tab next  Esc back\nChanging a running stack requires a stop and start.",
        ),
        Screen::GitHubMethod => render_menu(
            frame,
            vertical[1],
            "Connect GitHub",
            &[
                "Create private GitHub App",
                "Import existing GitHub App (Advanced)",
                "Fine-grained PAT (Deprecated)",
                "Back",
            ],
            app.selected,
        ),
        Screen::GitHubManifest => render_manifest(frame, vertical[1], app),
        Screen::GitHubInstall => render_install(frame, vertical[1], app),
        Screen::GitHubExisting => render_form(
            frame,
            vertical[1],
            "Import existing GitHub App",
            &[
                "App ID",
                "Installation ID",
                "Private key file",
                "Webhook secret file (if webhook)",
                "Public HTTPS URL (if webhook)",
                "Webhook ingress (Space)",
            ],
            &app.fields,
            app.field,
            false,
            "Enter load repositories  Tab next  Esc back",
        ),
        Screen::GitHubPat => render_form(
            frame,
            vertical[1],
            "Fine-grained PAT — deprecated and user-linked",
            &[
                "Token",
                "Public HTTPS URL (if webhook)",
                "Webhook ingress (Space)",
            ],
            &app.fields,
            app.field,
            true,
            "Enter load repositories  Tab next  Esc back",
        ),
        Screen::RepositorySelect => render_repositories(frame, vertical[1], app),
        Screen::GitHubAccessRepositories => {
            render_access_repositories(frame, vertical[1], app, instance)
        }
        Screen::GitHubAccessSubjects => render_access_subjects(frame, vertical[1], app, instance),
        Screen::GitHubAccessType => render_menu(
            frame,
            vertical[1],
            "Add trusted GitHub identity",
            &[
                "User",
                "Repository owner organization",
                "Owner team",
                "Back",
            ],
            app.selected,
        ),
        Screen::GitHubAccessForm => {
            let (title, labels): (&str, &[&str]) = match app.access_kind {
                GitHubAccessKind::User => ("Add trusted user", &["GitHub login"]),
                GitHubAccessKind::Organization => {
                    ("Trust repository owner organization", &["Organization"])
                }
                GitHubAccessKind::Team => (
                    "Add trusted organization team",
                    &["Organization", "Team slug"],
                ),
            };
            render_form(
                frame,
                vertical[1],
                title,
                labels,
                &app.fields,
                app.field,
                false,
                "Enter validate and save  Tab next  Esc back\nRunning APIs are recreated automatically so changes apply to new events.",
            );
        }
        Screen::CodexMethod => render_menu(
            frame,
            vertical[1],
            "Connect Codex",
            &["ChatGPT browser login", "OpenAI project API key", "Back"],
            app.selected,
        ),
        Screen::CodexApiKey => render_form(
            frame,
            vertical[1],
            "Codex API-key login",
            &["OpenAI project API key"],
            &app.fields,
            app.field,
            true,
            "Enter authenticate  Esc back",
        ),
        Screen::Doctor => render_doctor(frame, vertical[1], app),
        Screen::Plugins => render_plugins(frame, vertical[1], app, instance),
        Screen::PluginConnect => render_form(
            frame,
            vertical[1],
            "Connect local plugin",
            &[
                "Plugin directory or manifest",
                "Flow to activate (optional)",
                "Environment files (NAME=PATH, comma-separated)",
            ],
            &app.fields,
            app.field,
            false,
            "Enter validate/build  Tab next  Esc back\nValues are read from private files and never entered directly.",
        ),
    }
    let footer = app
        .busy
        .as_deref()
        .or(app.error.as_deref())
        .or(app.notice.as_deref())
        .unwrap_or(match app.screen {
            Screen::Home => "↑↓ select  Enter run  q quit",
            _ => "Tab/↑↓ navigate  Enter continue  Esc back",
        });
    let color = if app.error.is_some() {
        Color::Red
    } else if app.busy.is_some() {
        Color::Yellow
    } else {
        Color::DarkGray
    };
    frame.render_widget(
        Paragraph::new(footer)
            .style(Style::default().fg(color))
            .alignment(Alignment::Center)
            .block(Block::default().borders(Borders::TOP))
            .wrap(Wrap { trim: true }),
        vertical[2],
    );
    if app.busy.is_some() {
        let popup = centered_rect(76, 10.min(area.height.saturating_sub(2)), area);
        frame.render_widget(Clear, popup);
        frame.render_widget(
            Paragraph::new(app.busy.as_deref().unwrap())
                .alignment(Alignment::Center)
                .block(
                    Block::default()
                        .title(" Working ")
                        .borders(Borders::ALL)
                        .border_style(Style::default().fg(Color::Yellow)),
                )
                .wrap(Wrap { trim: true }),
            popup,
        );
    }
}

fn render_home(frame: &mut Frame, area: Rect, app: &App, instance: &Instance) {
    let columns = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(48), Constraint::Percentage(52)])
        .split(area);
    let config = instance.config();
    let github = config.and_then(|config| config.github.as_ref());
    let summary = vec![
        Line::from(format!(
            "Instance       {}",
            if config.is_some() {
                "configured"
            } else {
                "missing"
            }
        )),
        Line::from(format!(
            "GitHub        {}",
            match github {
                Some(GitHubInstanceConfig::App { .. }) => "App",
                Some(GitHubInstanceConfig::Pat { .. }) => "PAT (deprecated)",
                None => "not connected",
            }
        )),
        Line::from(format!(
            "Codex         {}",
            if config
                .and_then(|config| config.codex_home.as_ref())
                .is_some()
            {
                "connected"
            } else {
                "not connected"
            }
        )),
        Line::from(format!(
            "Ingress       {}",
            if app.polling_warning(instance) {
                "polling — delayed"
            } else if github.is_some() {
                "signed webhook"
            } else {
                "not configured"
            }
        )),
        Line::from(format!(
            "Access        {}",
            config
                .map(|config| {
                    let configured = config
                        .github_access
                        .values()
                        .filter(|subjects| !subjects.is_empty())
                        .count();
                    let total = config.github_access.len();
                    format!("{configured}/{total} repositories trusted")
                })
                .unwrap_or_else(|| "not configured".into())
        )),
        Line::from(format!(
            "Plugin        {}",
            config
                .and_then(|config| config.active_plugin.as_ref())
                .map(|plugin| format!("{}:{}", plugin.id, plugin.flow))
                .unwrap_or_else(|| "default lifecycle".into())
        )),
        Line::from(format!(
            "Dashboard     {}",
            config
                .map(|config| format!("http://127.0.0.1:{}", config.web_port))
                .unwrap_or_else(|| "not configured".into())
        )),
        Line::from(format!(
            "API           {}",
            config
                .map(|config| format!("http://127.0.0.1:{}", config.api_port))
                .unwrap_or_else(|| "not configured".into())
        )),
        Line::from(""),
        Line::styled(
            if app.status.running() {
                "● stack running"
            } else {
                "○ stack stopped or partial"
            },
            Style::default().fg(if app.status.running() {
                Color::Green
            } else {
                Color::Yellow
            }),
        ),
    ];
    frame.render_widget(
        Paragraph::new(summary)
            .block(Block::default().title(" Overview ").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        columns[0],
    );
    let right = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(7), Constraint::Length(9)])
        .split(columns[1]);
    let mut state = ListState::default();
    state.select(Some(app.selected));
    frame.render_stateful_widget(
        List::new(HOME_ACTIONS.iter().map(|action| ListItem::new(*action)))
            .block(Block::default().title(" Actions ").borders(Borders::ALL))
            .highlight_symbol("› ")
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        right[0],
        &mut state,
    );
    let services = if app.status.services.is_empty() {
        vec![ListItem::new("No running services")]
    } else {
        app.status
            .services
            .iter()
            .map(|service| {
                ListItem::new(format!(
                    "{:<10} {:<10} {}",
                    service.name,
                    service.state,
                    service.health.as_deref().unwrap_or("")
                ))
            })
            .collect()
    };
    frame.render_widget(
        List::new(services).block(
            Block::default()
                .title(" Services (2s refresh) ")
                .borders(Borders::ALL),
        ),
        right[1],
    );
}

fn render_access_repositories(frame: &mut Frame, area: Rect, app: &App, instance: &Instance) {
    let repositories = configured_github_repositories(instance);
    let items = repositories.iter().map(|repository| {
        let count = instance
            .github_access(repository)
            .map(|subjects| subjects.len())
            .unwrap_or(0);
        let status = if count == 0 {
            "DENY ALL".into()
        } else {
            format!("{count} trusted")
        };
        ListItem::new(format!("{repository:<40} {status}"))
    });
    let mut state = ListState::default();
    if !repositories.is_empty() {
        state.select(Some(app.selected.min(repositories.len() - 1)));
    }
    frame.render_stateful_widget(
        List::new(items)
            .block(
                Block::default()
                    .title(" GitHub access by repository ")
                    .borders(Borders::ALL),
            )
            .highlight_symbol("› ")
            .highlight_style(Style::default().fg(Color::Cyan)),
        padded(area, 3, 2),
        &mut state,
    );
}

fn render_access_subjects(frame: &mut Frame, area: Rect, app: &App, instance: &Instance) {
    let repository = app.access_repository.as_deref().unwrap_or("unknown");
    let subjects = instance.github_access(repository).unwrap_or_default();
    let items: Vec<ListItem> = if subjects.is_empty() {
        vec![ListItem::new(
            "DENY ALL — press a to add a trusted identity",
        )]
    } else {
        subjects
            .iter()
            .map(|subject| ListItem::new(subject.display_name()))
            .collect()
    };
    let mut state = ListState::default();
    if !subjects.is_empty() {
        state.select(Some(app.selected.min(subjects.len() - 1)));
    }
    frame.render_stateful_widget(
        List::new(items)
            .block(
                Block::default()
                    .title(format!(" Trusted identities — {repository} "))
                    .borders(Borders::ALL),
            )
            .highlight_symbol("› ")
            .highlight_style(Style::default().fg(Color::Cyan)),
        padded(area, 3, 2),
        &mut state,
    );
    frame.render_widget(
        Paragraph::new("a add  d remove  Esc repositories")
            .style(Style::default().fg(Color::Yellow)),
        Rect::new(
            area.x + 4,
            area.y + area.height.saturating_sub(3),
            area.width.saturating_sub(8),
            1,
        ),
    );
}

fn render_plugins(frame: &mut Frame, area: Rect, app: &App, instance: &Instance) {
    let flows = plugin_flows(instance);
    let active = instance
        .config()
        .and_then(|config| config.active_plugin.as_ref());
    let items = if flows.is_empty() {
        vec![ListItem::new(
            "No plugins installed. Press c to connect one.",
        )]
    } else {
        flows
            .iter()
            .map(|(id, flow, class)| {
                let marker =
                    if active.is_some_and(|active| active.id == *id && active.flow == *flow) {
                        "●"
                    } else {
                        "○"
                    };
                ListItem::new(format!("{marker} {id}:{flow}  {class:?}"))
            })
            .collect()
    };
    let mut state = ListState::default();
    if !flows.is_empty() {
        state.select(Some(app.selected.min(flows.len() - 1)));
    }
    frame.render_stateful_widget(
        List::new(items)
            .block(
                Block::default()
                    .title(" Plugins — one active flow ")
                    .borders(Borders::ALL),
            )
            .highlight_symbol("› ")
            .highlight_style(Style::default().fg(Color::Cyan)),
        padded(area, 2, 1),
        &mut state,
    );
    frame.render_widget(
        Paragraph::new(
            "Enter activate  c connect/reconfigure  r rebuild  d disable  Esc back\nLifecycle-replacement flows are exclusive and expose the Docker socket to the worker while active.",
        )
        .style(Style::default().fg(Color::Yellow)),
        Rect::new(area.x + 4, area.y + area.height.saturating_sub(4), area.width.saturating_sub(8), 3),
    );
}

fn render_menu(frame: &mut Frame, area: Rect, title: &str, options: &[&str], selected: usize) {
    let mut state = ListState::default();
    state.select(Some(selected));
    frame.render_stateful_widget(
        List::new(options.iter().map(|option| ListItem::new(*option)))
            .block(
                Block::default()
                    .title(format!(" {title} "))
                    .borders(Borders::ALL),
            )
            .highlight_symbol("› ")
            .highlight_style(
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD),
            ),
        padded(area, 4, 2),
        &mut state,
    );
}

fn render_form(
    frame: &mut Frame,
    area: Rect,
    title: &str,
    labels: &[&str],
    fields: &[String],
    selected: usize,
    secret: bool,
    help: &str,
) {
    let mut lines = Vec::new();
    for (index, label) in labels.iter().enumerate() {
        let value = fields
            .get(index)
            .map(|value| {
                if secret && index == 0 {
                    "•".repeat(value.chars().count())
                } else {
                    value.clone()
                }
            })
            .unwrap_or_else(|| {
                if index == labels.len() - 1 {
                    "[ ]".into()
                } else {
                    String::new()
                }
            });
        lines.push(Line::from(vec![
            Span::styled(
                if index == selected { "› " } else { "  " },
                Style::default().fg(Color::Cyan),
            ),
            Span::styled(
                format!("{label}: "),
                Style::default().add_modifier(Modifier::BOLD),
            ),
            Span::raw(value),
        ]));
        lines.push(Line::from(""));
    }
    lines.push(Line::styled(help, Style::default().fg(Color::DarkGray)));
    frame.render_widget(
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(format!(" {title} "))
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: false }),
        padded(area, 2, 1),
    );
}

fn render_manifest(frame: &mut Frame, area: Rect, app: &App) {
    let fields = vec![
        app.fields.first().cloned().unwrap_or_default(),
        app.fields.get(1).cloned().unwrap_or_default(),
        app.fields.get(2).cloned().unwrap_or_default(),
        if app.webhook {
            "[x]".into()
        } else {
            "[ ]".into()
        },
    ];
    render_form(
        frame,
        area,
        "Create private GitHub App",
        &[
            "Repository owner",
            "Organization (blank for personal)",
            "Public HTTPS URL (if webhook)",
            "Webhook ingress (Space)",
        ],
        &fields,
        app.field,
        false,
        "Enter starts registration  Tab next  Esc back\nHeadless: first run `ssh -N -L 8787:127.0.0.1:8787 USER@HOST` on your workstation, then open http://127.0.0.1:8787/ there.\nPolling is delayed and not real-time.",
    );
}

fn render_install(frame: &mut Frame, area: Rect, app: &App) {
    let text = if let Some(pending) = &app.pending {
        format!(
            "GitHub App `{}` was registered for `{}`.\n\nComplete repository installation in the browser:\nhttps://github.com/apps/{}/installations/new\n\nThe registration is stored privately and can be resumed after interruption.\n\nEnter  discover installed repositories\nd      discard pending registration\nEsc    authentication choices",
            pending.slug, pending.owner, pending.slug
        )
    } else {
        "Pending registration could not be loaded.".into()
    };
    frame.render_widget(
        Paragraph::new(text)
            .block(
                Block::default()
                    .title(" Install GitHub App ")
                    .borders(Borders::ALL),
            )
            .wrap(Wrap { trim: true }),
        padded(area, 4, 2),
    );
}

fn render_repositories(frame: &mut Frame, area: Rect, app: &App) {
    let items = app
        .repositories
        .iter()
        .zip(&app.repository_selected)
        .map(|(repository, selected)| {
            ListItem::new(format!(
                "[{}] {}{}",
                if *selected { "x" } else { " " },
                repository.full_name,
                if repository.private { "  private" } else { "" }
            ))
        })
        .collect::<Vec<_>>();
    let mut state = ListState::default();
    if !items.is_empty() {
        state.select(Some(app.repository_cursor));
    }
    frame.render_stateful_widget(
        List::new(items)
            .block(
                Block::default()
                    .title(" Select repositories — one owner per instance ")
                    .borders(Borders::ALL),
            )
            .highlight_symbol("› ")
            .highlight_style(Style::default().fg(Color::Cyan)),
        padded(area, 2, 1),
        &mut state,
    );
}

fn render_doctor(frame: &mut Frame, area: Rect, app: &App) {
    let lines = match &app.doctor {
        Some(report) => report
            .checks
            .iter()
            .map(|check| {
                let (symbol, color) = match check.level {
                    CheckLevel::Pass => ("✓", Color::Green),
                    CheckLevel::Warning => ("!", Color::Yellow),
                    CheckLevel::Fail => ("×", Color::Red),
                };
                Line::from(vec![
                    Span::styled(
                        format!("{symbol} "),
                        Style::default().fg(color).add_modifier(Modifier::BOLD),
                    ),
                    Span::styled(
                        format!("{}: ", check.name),
                        Style::default().add_modifier(Modifier::BOLD),
                    ),
                    Span::raw(&check.detail),
                ])
            })
            .chain(std::iter::once(Line::from("")))
            .chain(std::iter::once(Line::styled(
                if report.passed() {
                    "Enter  Start Donkeyspace    r  rerun    b  back"
                } else {
                    "r  rerun diagnostics    b  back"
                },
                Style::default().fg(Color::Cyan),
            )))
            .collect(),
        None => vec![
            Line::from("Diagnostics have not run yet."),
            Line::from("Press r to run them."),
        ],
    };
    frame.render_widget(
        Paragraph::new(lines)
            .block(Block::default().title(" Doctor ").borders(Borders::ALL))
            .wrap(Wrap { trim: true }),
        padded(area, 2, 1),
    );
}

fn padded(area: Rect, horizontal: u16, vertical: u16) -> Rect {
    let vertical_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(vertical),
            Constraint::Min(1),
            Constraint::Length(vertical),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(horizontal),
            Constraint::Min(1),
            Constraint::Length(horizontal),
        ])
        .split(vertical_layout[1])[1]
}

fn centered_rect(width_percent: u16, height: u16, area: Rect) -> Rect {
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Fill(1),
            Constraint::Length(height),
            Constraint::Fill(1),
        ])
        .split(area);
    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - width_percent) / 2),
            Constraint::Percentage(width_percent),
            Constraint::Percentage((100 - width_percent) / 2),
        ])
        .split(vertical[1])[1]
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::{Terminal, backend::TestBackend};

    fn app(screen: Screen) -> App {
        App {
            config_dir: None,
            screen,
            selected: 0,
            fields: vec![String::new()],
            field: 0,
            webhook: false,
            busy: None,
            notice: None,
            error: None,
            status: DeploymentStatus { services: vec![] },
            doctor: None,
            repositories: vec![],
            repository_selected: vec![],
            repository_cursor: 0,
            github_flow: GitHubFlow::Pending,
            pending: None,
            installation_id: None,
            access_repository: None,
            access_kind: GitHubAccessKind::User,
            confirm_remove: false,
            continue_setup_after_access: false,
            pat: None,
            should_quit: false,
            last_refresh: Instant::now(),
        }
    }

    #[test]
    fn home_navigation_is_bounded() {
        let mut app = app(Screen::Home);
        let (sender, _receiver) = mpsc::unbounded_channel();
        handle_home(
            &mut app,
            KeyEvent::new(KeyCode::Up, KeyModifiers::NONE),
            sender.clone(),
        );
        assert_eq!(app.selected, 0);
        for _ in 0..20 {
            handle_home(
                &mut app,
                KeyEvent::new(KeyCode::Down, KeyModifiers::NONE),
                sender.clone(),
            );
        }
        assert_eq!(app.selected, HOME_ACTIONS.len() - 1);
    }

    #[test]
    fn validates_port_fields() {
        assert_eq!(parse_port("8081", "API port").unwrap(), 8081);
        assert!(parse_port("0", "API port").is_err());
        assert!(parse_port("not-a-port", "API port").is_err());
    }

    #[test]
    fn access_team_form_is_scoped_to_repository_owner() {
        let mut app = app(Screen::GitHubAccessType);
        app.access_repository = Some("acme/rtl".into());
        app.selected = 2;
        handle_access_type(&mut app, KeyEvent::from(KeyCode::Enter)).unwrap();
        assert_eq!(app.screen, Screen::GitHubAccessForm);
        assert_eq!(app.access_kind, GitHubAccessKind::Team);
        assert_eq!(app.fields, vec!["acme", ""]);
    }

    #[test]
    fn uninitialized_app_prefills_distinct_available_ports() {
        let directory =
            std::env::temp_dir().join(format!("donkeyspace-tui-port-test-{}", std::process::id()));
        let instance = Instance::open(Some(directory)).unwrap();
        let app = App::new(&instance, None).unwrap();
        assert_eq!(app.screen, Screen::Init);
        assert_eq!(app.fields.len(), 3);
        assert_ne!(app.fields[1], app.fields[2]);
        assert!(parse_port(&app.fields[1], "API port").is_ok());
        assert!(parse_port(&app.fields[2], "Dashboard port").is_ok());
    }

    #[test]
    fn repository_multiselect_toggles_without_exposing_secrets() {
        let mut app = app(Screen::RepositorySelect);
        app.repositories.push(GitHubRepository {
            owner: "owner".into(),
            name: "repo".into(),
            full_name: "owner/repo".into(),
            private: true,
        });
        app.repository_selected.push(false);
        let (sender, _receiver) = mpsc::unbounded_channel();
        handle_repositories(
            &mut app,
            KeyEvent::new(KeyCode::Char(' '), KeyModifiers::NONE),
            sender,
        );
        assert!(app.repository_selected[0]);
    }

    #[test]
    fn renders_home_and_narrow_forms() {
        let directory =
            std::env::temp_dir().join(format!("donkeyspace-tui-render-{}", std::process::id()));
        let instance = Instance::open(Some(directory)).unwrap();
        for (screen, width) in [(Screen::Home, 80), (Screen::GitHubPat, 42)] {
            let backend = TestBackend::new(width, 24);
            let mut terminal = Terminal::new(backend).unwrap();
            let app = app(screen);
            terminal
                .draw(|frame| render(frame, &app, &instance))
                .unwrap();
        }
    }

    #[test]
    fn secret_field_is_masked_in_render_buffer() {
        let backend = TestBackend::new(60, 16);
        let mut terminal = Terminal::new(backend).unwrap();
        terminal
            .draw(|frame| {
                render_form(
                    frame,
                    frame.area(),
                    "Secret",
                    &["Token"],
                    &["ghp_hidden".into()],
                    0,
                    true,
                    "help",
                )
            })
            .unwrap();
        let buffer = terminal.backend().buffer();
        let rendered = buffer
            .content
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>();
        assert!(!rendered.contains("ghp_hidden"));
    }

    #[test]
    fn plugin_environment_accepts_only_file_references() {
        let values = parse_plugin_environment_files("TOKEN=/tmp/token, MODE=/tmp/mode").unwrap();
        assert_eq!(values.len(), 2);
        assert!(matches!(
            values.get("TOKEN"),
            Some(PluginEnvironmentInput::File(path)) if path == &PathBuf::from("/tmp/token")
        ));
        assert!(parse_plugin_environment_files("TOKEN").is_err());
    }
}
