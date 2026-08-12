use clap::{Args, Parser, Subcommand, ValueEnum};
use donkeyspace_cli::{
    CodexLoginMethod, ConnectGitHubOptions, Instance, RuntimeSource, SetupError,
};
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
    Doctor,
    Up,
    Down,
    Status,
    Reset(ResetArgs),
}

#[derive(Debug, Args)]
struct InitArgs {
    #[arg(long, default_value = ".")]
    source_tree: PathBuf,
    #[arg(long, value_enum, default_value_t = SourceArg::LocalBuild)]
    runtime_source: SourceArg,
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
            instance.init(args.source_tree, source)?;
            println!("initialized {}", instance.config_path().display());
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
        },
        Command::Doctor => instance.doctor().await?,
        Command::Up => instance.up()?,
        Command::Down => instance.down()?,
        Command::Status => instance.status()?,
        Command::Reset(args) => instance.reset(args.delete_data, args.confirm)?,
    }
    Ok(())
}
