use clap::{Args, Parser, Subcommand, ValueEnum};
use donkeyspace_cli::{
    CodexLoginMethod, ConnectGitHubOptions, GitHubAccessSubject, Instance, PluginConnectOptions,
    PluginEnvironmentInput, RuntimeSource, SetupError,
};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "donkeyspace",
    about = "Install and operate a local Donkeyspace deployment"
)]
struct Cli {
    #[arg(long, global = true)]
    config_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, Subcommand)]
enum Command {
    Init(InitArgs),
    Connect(ConnectArgs),
    Configure(ConfigureArgs),
    Doctor,
    Up,
    Down,
    Status,
    Plugin(PluginArgs),
    Reset(ResetArgs),
}

#[derive(Debug, Args)]
struct InitArgs {
    #[arg(long, default_value = ".")]
    source_tree: PathBuf,
    #[arg(long, value_enum, default_value_t = SourceArg::LocalBuild)]
    runtime_source: SourceArg,
    #[arg(long)]
    api_port: Option<u16>,
    #[arg(long)]
    web_port: Option<u16>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum SourceArg {
    LocalBuild,
    RegistryImage,
}

#[derive(Debug, Args)]
struct ConnectArgs {
    #[command(subcommand)]
    target: ConnectTarget,
}

#[derive(Debug, Subcommand)]
enum ConnectTarget {
    Github(GitHubArgs),
    Codex(CodexArgs),
    Plugin(ConnectPluginArgs),
}

#[derive(Debug, Args)]
struct ConfigureArgs {
    #[command(subcommand)]
    target: ConfigureTarget,
}

#[derive(Debug, Subcommand)]
enum ConfigureTarget {
    Ports(PortArgs),
    GithubAccess(GitHubAccessArgs),
}

#[derive(Debug, Args)]
struct GitHubAccessArgs {
    #[arg(long)]
    repository: String,
    #[command(subcommand)]
    command: GitHubAccessCommand,
}

#[derive(Debug, Subcommand)]
enum GitHubAccessCommand {
    List,
    Add(AccessSubjectArgs),
    Remove(AccessSubjectArgs),
}

#[derive(Debug, Args)]
#[group(required = true, multiple = false)]
struct AccessSubjectArgs {
    #[arg(long)]
    user: Option<String>,
    #[arg(long)]
    organization: Option<String>,
    #[arg(long, value_name = "ORG/TEAM-SLUG")]
    team: Option<String>,
}

impl AccessSubjectArgs {
    fn subject(self) -> Result<GitHubAccessSubject, SetupError> {
        match (self.user, self.organization, self.team) {
            (Some(login), None, None) => Ok(GitHubAccessSubject::User { login }),
            (None, Some(login), None) => Ok(GitHubAccessSubject::Organization { login }),
            (None, None, Some(team)) => {
                let (organization, team_slug) = team
                    .split_once('/')
                    .ok_or_else(|| SetupError::Config("--team must use ORG/TEAM-SLUG".into()))?;
                if organization.is_empty() || team_slug.is_empty() || team_slug.contains('/') {
                    return Err(SetupError::Config("--team must use ORG/TEAM-SLUG".into()));
                }
                Ok(GitHubAccessSubject::Team {
                    organization: organization.into(),
                    team_slug: team_slug.into(),
                })
            }
            _ => Err(SetupError::Config(
                "provide exactly one of --user, --organization, or --team".into(),
            )),
        }
    }
}

#[derive(Debug, Args)]
struct PortArgs {
    #[arg(long)]
    api_port: Option<u16>,
    #[arg(long)]
    web_port: Option<u16>,
}

#[derive(Debug, Args)]
struct ConnectPluginArgs {
    #[arg(long)]
    path: PathBuf,
    #[arg(long)]
    flow: Option<String>,
    #[arg(long = "environment-file", value_name = "NAME=PATH")]
    environment_files: Vec<String>,
}

#[derive(Debug, Args)]
struct PluginArgs {
    #[command(subcommand)]
    command: PluginCommand,
}

#[derive(Debug, Subcommand)]
enum PluginCommand {
    List,
    Activate {
        id: String,
        #[arg(long)]
        flow: String,
    },
    Rebuild {
        id: String,
    },
    Disable,
}

#[derive(Debug, Args)]
struct GitHubArgs {
    #[arg(long)]
    app_id: Option<u64>,
    #[arg(long)]
    installation_id: Option<u64>,
    #[arg(long)]
    private_key_file: Option<PathBuf>,
    #[arg(long)]
    webhook_secret_file: Option<PathBuf>,
    #[arg(long, value_delimiter = ',')]
    repositories: Vec<String>,
    #[arg(long)]
    public_url: Option<String>,
    #[arg(long, default_value_t = 8787)]
    callback_port: u16,
    #[arg(long)]
    organization: Option<String>,
    #[arg(long)]
    pat: bool,
}

#[derive(Debug, Args)]
struct CodexArgs {
    #[arg(long, value_enum, default_value_t = CodexArg::Chatgpt)]
    method: CodexArg,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum CodexArg {
    Chatgpt,
    ApiKey,
}

#[derive(Debug, Args)]
struct ResetArgs {
    #[arg(long)]
    delete_data: bool,
    #[arg(long)]
    confirm: bool,
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), SetupError> {
    let cli = Cli::parse();
    if cli.command.is_none() {
        return donkeyspace_cli::tui::run(cli.config_dir).await;
    }
    let mut instance = Instance::open(cli.config_dir)?;
    match cli.command.expect("checked above") {
        Command::Init(args) => {
            let source = match args.runtime_source {
                SourceArg::LocalBuild => RuntimeSource::LocalBuild,
                SourceArg::RegistryImage => RuntimeSource::RegistryImage,
            };
            instance.init_with_ports(args.source_tree, source, args.api_port, args.web_port)?;
            println!("initialized {}", instance.config_path().display());
            println!("Dashboard: {}", instance.dashboard_url()?);
            println!("API: {}", instance.api_url()?);
        }
        Command::Connect(args) => match args.target {
            ConnectTarget::Github(args) => {
                let options = ConnectGitHubOptions {
                    app_id: args.app_id,
                    installation_id: args.installation_id,
                    private_key_file: args.private_key_file,
                    webhook_secret_file: args.webhook_secret_file,
                    repositories: args.repositories,
                    public_url: args.public_url,
                    callback_port: args.callback_port,
                    organization: args.organization,
                    pat: args.pat,
                };
                instance.connect_github(options).await?;
            }
            ConnectTarget::Codex(args) => {
                let method = match args.method {
                    CodexArg::Chatgpt => CodexLoginMethod::ChatGpt,
                    CodexArg::ApiKey => CodexLoginMethod::ApiKey,
                };
                instance.connect_codex(method)?;
            }
            ConnectTarget::Plugin(args) => {
                let mut environment = BTreeMap::new();
                for item in args.environment_files {
                    let (name, path) = item.split_once('=').ok_or_else(|| {
                        SetupError::Config("--environment-file must use NAME=PATH".into())
                    })?;
                    if name.is_empty() || path.is_empty() {
                        return Err(SetupError::Config(
                            "--environment-file must use NAME=PATH".into(),
                        ));
                    }
                    environment.insert(
                        name.to_string(),
                        PluginEnvironmentInput::File(PathBuf::from(path)),
                    );
                }
                instance.connect_plugin(PluginConnectOptions {
                    path: args.path,
                    flow: args.flow,
                    environment,
                })?;
                println!("plugin connected");
            }
        },
        Command::Configure(args) => match args.target {
            ConfigureTarget::Ports(args) => {
                let stack_running = instance
                    .deployment_status()
                    .map(|status| {
                        status
                            .services
                            .iter()
                            .any(|service| service.state.eq_ignore_ascii_case("running"))
                    })
                    .unwrap_or(false);
                instance.configure_ports(args.api_port, args.web_port)?;
                println!("ports saved");
                println!("Dashboard: {}", instance.dashboard_url()?);
                println!("API: {}", instance.api_url()?);
                if stack_running {
                    println!(
                        "restart required: run `donkeyspace down` followed by `donkeyspace up`"
                    );
                }
            }
            ConfigureTarget::GithubAccess(args) => match args.command {
                GitHubAccessCommand::List => {
                    let subjects = instance.github_access(&args.repository)?;
                    if subjects.is_empty() {
                        println!("deny all: no trusted subjects configured");
                    }
                    for subject in subjects {
                        println!("{}", subject.display_name());
                    }
                }
                GitHubAccessCommand::Add(subject) => {
                    let subject = instance
                        .add_github_access(&args.repository, subject.subject()?)
                        .await?;
                    println!("added {} to {}", subject.display_name(), args.repository);
                }
                GitHubAccessCommand::Remove(subject) => {
                    let subject = subject.subject()?;
                    instance.remove_github_access(&args.repository, &subject)?;
                    println!(
                        "removed {} from {}",
                        subject.display_name(),
                        args.repository
                    );
                    if instance.github_access(&args.repository)?.is_empty() {
                        println!("warning: repository now denies all engagement");
                    }
                }
            },
        },
        Command::Doctor => instance.doctor().await?,
        Command::Up => instance.up()?,
        Command::Down => instance.down()?,
        Command::Status => instance.status()?,
        Command::Plugin(args) => match args.command {
            PluginCommand::List => {
                let config = instance.config().ok_or_else(|| {
                    SetupError::Config("instance is not initialized; run `donkeyspace init`".into())
                })?;
                if config.plugins.is_empty() {
                    println!("no plugins installed");
                }
                for plugin in config.plugins.values() {
                    let active = config
                        .active_plugin
                        .as_ref()
                        .filter(|active| active.id == plugin.id)
                        .map(|active| format!(" active:{}", active.flow))
                        .unwrap_or_default();
                    println!("{}{} ({})", plugin.id, active, plugin.source_path.display());
                    for (flow, class) in &plugin.flows {
                        println!("  {flow}: {class:?}");
                    }
                }
            }
            PluginCommand::Activate { id, flow } => {
                instance.activate_plugin(&id, &flow)?;
                println!("activated {id}:{flow}");
            }
            PluginCommand::Rebuild { id } => {
                instance.rebuild_plugin(&id)?;
                println!("rebuilt {id}");
            }
            PluginCommand::Disable => {
                instance.disable_plugin()?;
                println!("plugin flow disabled; installed plugins were preserved");
            }
        },
        Command::Reset(args) => instance.reset(args.delete_data, args.confirm)?,
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_init_and_reconfigure_port_flags() {
        let init = Cli::try_parse_from([
            "donkeyspace",
            "init",
            "--api-port",
            "8081",
            "--web-port",
            "5174",
        ])
        .unwrap();
        let Some(Command::Init(args)) = init.command else {
            panic!("expected init command");
        };
        assert_eq!(args.api_port, Some(8081));
        assert_eq!(args.web_port, Some(5174));

        let configure =
            Cli::try_parse_from(["donkeyspace", "configure", "ports", "--web-port", "5175"])
                .unwrap();
        let Some(Command::Configure(ConfigureArgs {
            target: ConfigureTarget::Ports(args),
        })) = configure.command
        else {
            panic!("expected configure ports command");
        };
        assert_eq!(args.api_port, None);
        assert_eq!(args.web_port, Some(5175));
    }

    #[test]
    fn parses_repository_access_subjects() {
        let cli = Cli::try_parse_from([
            "donkeyspace",
            "configure",
            "github-access",
            "--repository",
            "acme/rtl",
            "add",
            "--team",
            "acme/hardware",
        ])
        .unwrap();
        let Some(Command::Configure(ConfigureArgs {
            target: ConfigureTarget::GithubAccess(args),
        })) = cli.command
        else {
            panic!("expected GitHub access command");
        };
        assert_eq!(args.repository, "acme/rtl");
        let GitHubAccessCommand::Add(subject) = args.command else {
            panic!("expected add command");
        };
        assert_eq!(
            subject.subject().unwrap(),
            GitHubAccessSubject::Team {
                organization: "acme".into(),
                team_slug: "hardware".into(),
            }
        );
    }
}
