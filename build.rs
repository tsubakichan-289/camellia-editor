use std::env;
use std::path::PathBuf;

fn main() {
    println!("cargo:rerun-if-changed=tsubaki.jpg");

    if env::var("CARGO_CFG_TARGET_OS").as_deref() != Ok("windows") {
        return;
    }

    if let Err(err) = embed_windows_icon() {
        panic!("failed to embed windows icon: {err}");
    }
}

fn embed_windows_icon() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let icon_source = manifest_dir.join("tsubaki.jpg");
    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let icon_path = out_dir.join("app-icon.ico");

    let image = image::open(&icon_source)?
        .resize_to_fill(256, 256, image::imageops::FilterType::Lanczos3);
    image.save_with_format(&icon_path, image::ImageFormat::Ico)?;

    let mut resource = winresource::WindowsResource::new();
    resource.set_icon(icon_path.to_string_lossy().as_ref());
    resource.compile()?;
    Ok(())
}
