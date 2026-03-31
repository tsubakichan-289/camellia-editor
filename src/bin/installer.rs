use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let target_triple =
        env::var("TARGET_TRIPLE").unwrap_or_else(|_| "x86_64-pc-windows-gnu".to_owned());
    let profile = env::var("PROFILE").unwrap_or_else(|_| "release".to_owned());
    let app_dir = root.join("camellia-editor");
    let exe_name = "camellia-editor.exe";
    let target_exe = root
        .join("target")
        .join(&target_triple)
        .join(&profile)
        .join(exe_name);
    let iss_file = root.join("camellia-editor.iss");

    println!("[1/4] cargo build --{} --target {}", profile, target_triple);
    run_status(
        Command::new("cargo")
            .current_dir(&root)
            .arg("build")
            .arg(format!("--{profile}"))
            .arg("--target")
            .arg(&target_triple),
        "cargo build",
    )?;

    if !target_exe.is_file() {
        return Err(format!("missing target exe: {}", target_exe.display()));
    }

    println!("[2/4] update packaged exe");
    let packaged_exe = app_dir.join(exe_name);
    fs::create_dir_all(&app_dir)
        .map_err(|err| format!("could not create app dir {}: {err}", app_dir.display()))?;
    fs::copy(&target_exe, &packaged_exe).map_err(|err| {
        format!(
            "could not copy {} to {}: {err}",
            target_exe.display(),
            packaged_exe.display()
        )
    })?;

    println!("[3/4] verify installer inputs");
    for required in [
        app_dir.join("camellia-editor.exe"),
        app_dir.join("hunspell.exe"),
        app_dir.join("texlab.exe"),
        app_dir.join("en_US.aff"),
        app_dir.join("en_US.dic"),
    ] {
        if !required.exists() {
            return Err(format!("missing required file: {}", required.display()));
        }
    }

    let iscc_exe = resolve_iscc_exe()?;
    let iss_arg = to_windows_path(&iss_file)?;

    println!("[4/4] build installer");
    run_status(
        Command::new(&iscc_exe)
            .current_dir(&root)
            .arg(&iss_arg),
        "ISCC.exe",
    )?;

    println!();
    println!("installer ready:");
    println!("  {}", root.join("installer-dist").join("camellia-editor-setup.exe").display());

    Ok(())
}

fn run_status(command: &mut Command, label: &str) -> Result<(), String> {
    let status = command
        .status()
        .map_err(|err| format!("failed to launch {label}: {err}"))?;
    ensure_success(status, label)
}

fn ensure_success(status: ExitStatus, label: &str) -> Result<(), String> {
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} exited with {status}"))
    }
}

fn resolve_iscc_exe() -> Result<PathBuf, String> {
    if let Ok(path) = env::var("ISCC_EXE") {
        let path = PathBuf::from(path);
        if path.is_file() {
            return Ok(path);
        }
    }

    let candidates = [
        "/mnt/c/Users/jyant/AppData/Local/Programs/Inno Setup 6/ISCC.exe",
        "/mnt/c/Users/jyanto/AppData/Local/Programs/Inno Setup 6/ISCC.exe",
        "/mnt/c/Program Files (x86)/Inno Setup 6/ISCC.exe",
        "/mnt/c/Program Files/Inno Setup 6/ISCC.exe",
    ];

    candidates
        .iter()
        .map(PathBuf::from)
        .find(|path| path.is_file())
        .ok_or_else(|| "ISCC.exe not found. Set ISCC_EXE to the full path.".to_owned())
}

fn to_windows_path(path: &Path) -> Result<String, String> {
    let output = Command::new("wslpath")
        .arg("-w")
        .arg(path)
        .output()
        .map_err(|err| format!("failed to launch wslpath: {err}"))?;
    if !output.status.success() {
        return Err(format!("wslpath exited with {}", output.status));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}
