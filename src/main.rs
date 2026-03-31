#![cfg_attr(target_os = "windows", windows_subsystem = "windows")]

use std::fs;
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::BufRead;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{ChildStdin, ChildStdout, Command};
use std::process::Stdio;
use std::sync::OnceLock;
use std::sync::mpsc::{self, Receiver, Sender, TryRecvError};
use std::time::{Duration, Instant, UNIX_EPOCH};
#[cfg(target_os = "windows")]
use std::os::windows::process::CommandExt;

use eframe::egui::{
    self, Align, Color32, FontDefinitions, FontFamily, IconData, Key, KeyboardShortcut, Layout,
    Modifiers, RichText, ScrollArea, TextEdit, TextFormat, TextureHandle, TextureOptions,
    ViewportCommand, text::LayoutJob,
};
use eframe::egui::text::{CCursor, CCursorRange};
use eframe::{App, Frame, NativeOptions};
use rfd::FileDialog;
use serde_json::{Value, json};

const BUNDLED_LATEXMKRC: &str =
    include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/assets/latexmkrc"));
const MATH_PREVIEW_IDLE_DELAY: Duration = Duration::from_millis(50);
#[cfg(target_os = "windows")]
const CREATE_NO_WINDOW: u32 = 0x08000000;

fn main() -> eframe::Result<()> {
    let options = NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([1380.0, 860.0])
            .with_min_inner_size([960.0, 640.0])
            .with_icon(load_app_icon()),
        ..Default::default()
    };

    eframe::run_native(
        "camellia-editor",
        options,
        Box::new(|cc| {
            configure_fonts(&cc.egui_ctx);
            Ok(Box::<TexEditorApp>::default())
        }),
    )
}

fn load_app_icon() -> IconData {
    let bytes = include_bytes!(concat!(env!("CARGO_MANIFEST_DIR"), "/tsubaki.jpg"));
    let image = image::load_from_memory(bytes)
        .expect("failed to decode tsubaki.jpg")
        .into_rgba8();
    let (width, height) = image.dimensions();
    IconData {
        rgba: image.into_raw(),
        width,
        height,
    }
}

fn app_state_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        if let Ok(appdata) = std::env::var("APPDATA") {
            return Some(PathBuf::from(appdata).join("camellia-editor"));
        }
    }
    #[cfg(not(target_os = "windows"))]
    {
        if let Ok(config_home) = std::env::var("XDG_CONFIG_HOME") {
            return Some(PathBuf::from(config_home).join("camellia-editor"));
        }
        if let Ok(home) = std::env::var("HOME") {
            return Some(PathBuf::from(home).join(".config").join("camellia-editor"));
        }
    }
    None
}

fn recent_directories_path() -> Option<PathBuf> {
    app_state_dir().map(|dir| dir.join("recent-directories.json"))
}

fn settings_path() -> Option<PathBuf> {
    app_state_dir().map(|dir| dir.join("settings.json"))
}

fn writable_templates_path() -> Option<PathBuf> {
    app_state_dir().map(|dir| dir.join("templates"))
}

fn load_recent_directories() -> Vec<PathBuf> {
    let Some(path) = recent_directories_path() else {
        return Vec::new();
    };
    let Ok(text) = fs::read_to_string(path) else {
        return Vec::new();
    };
    let Ok(items) = serde_json::from_str::<Vec<String>>(&text) else {
        return Vec::new();
    };
    items
        .into_iter()
        .map(PathBuf::from)
        .filter(|path| path.is_dir())
        .collect()
}

fn save_recent_directories(paths: &[PathBuf]) {
    let Some(path) = recent_directories_path() else {
        return;
    };
    let Some(parent) = path.parent() else {
        return;
    };
    if fs::create_dir_all(parent).is_err() {
        return;
    }
    let serialized: Vec<String> = paths
        .iter()
        .map(|path| path.to_string_lossy().into_owned())
        .collect();
    if let Ok(json) = serde_json::to_string(&serialized) {
        let _ = fs::write(path, json);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum BuildToolPreference {
    Auto,
    Latexmk,
    Tectonic,
}

impl BuildToolPreference {
    fn label(self) -> &'static str {
        match self {
            Self::Auto => "Auto",
            Self::Latexmk => "latexmk",
            Self::Tectonic => "tectonic",
        }
    }

    fn from_str(value: &str) -> Self {
        match value {
            "latexmk" => Self::Latexmk,
            "tectonic" => Self::Tectonic,
            _ => Self::Auto,
        }
    }

    fn as_str(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::Latexmk => "latexmk",
            Self::Tectonic => "tectonic",
        }
    }
}

#[derive(Clone)]
struct AppSettings {
    spellcheck_enabled: bool,
    preferred_dictionary: Option<String>,
    build_tool: BuildToolPreference,
    open_pdf_after_build: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            spellcheck_enabled: true,
            preferred_dictionary: None,
            build_tool: BuildToolPreference::Auto,
            open_pdf_after_build: true,
        }
    }
}

fn load_app_settings() -> AppSettings {
    let Some(path) = settings_path() else {
        return AppSettings::default();
    };
    let Ok(text) = fs::read_to_string(path) else {
        return AppSettings::default();
    };
    let Ok(value) = serde_json::from_str::<Value>(&text) else {
        return AppSettings::default();
    };

    let mut settings = AppSettings::default();
    if let Some(enabled) = value.get("spellcheck_enabled").and_then(Value::as_bool) {
        settings.spellcheck_enabled = enabled;
    }
    if let Some(dictionary) = value.get("preferred_dictionary").and_then(Value::as_str) {
        let dictionary = dictionary.trim();
        if !dictionary.is_empty() {
            settings.preferred_dictionary = Some(dictionary.to_owned());
        }
    }
    if let Some(build_tool) = value.get("build_tool").and_then(Value::as_str) {
        settings.build_tool = BuildToolPreference::from_str(build_tool);
    }
    if let Some(open_pdf_after_build) = value.get("open_pdf_after_build").and_then(Value::as_bool) {
        settings.open_pdf_after_build = open_pdf_after_build;
    }
    settings
}

fn save_app_settings(settings: &AppSettings) {
    let Some(path) = settings_path() else {
        return;
    };
    let Some(parent) = path.parent() else {
        return;
    };
    if fs::create_dir_all(parent).is_err() {
        return;
    }

    let value = json!({
        "spellcheck_enabled": settings.spellcheck_enabled,
        "preferred_dictionary": settings.preferred_dictionary,
        "build_tool": settings.build_tool.as_str(),
        "open_pdf_after_build": settings.open_pdf_after_build,
    });
    if let Ok(text) = serde_json::to_string_pretty(&value) {
        let _ = fs::write(path, text);
    }
}

fn launch_target_path() -> Option<PathBuf> {
    std::env::args_os()
        .nth(1)
        .map(PathBuf::from)
        .filter(|path| path.exists())
}

fn configure_command(command: Command) -> Command {
    #[cfg(target_os = "windows")]
    {
        let mut command = command;
        command.creation_flags(CREATE_NO_WINDOW);
        return command;
    }
    #[cfg(not(target_os = "windows"))]
    {
        command
    }
}

struct TexEditorApp {
    settings: AppSettings,
    show_settings_window: bool,
    show_templates_window: bool,
    text: String,
    current_path: Option<PathBuf>,
    file_buffers: HashMap<PathBuf, EditorBuffer>,
    opened_directory: Option<PathBuf>,
    recent_directories: Vec<PathBuf>,
    file_tree: FileNode,
    file_tree_dirty: bool,
    active_left_tab: LeftPanelTab,
    active_center_tab: CenterPanelTab,
    git_status: String,
    dirty: bool,
    status_message: String,
    build_log: String,
    build_tool: Option<String>,
    build_receiver: Option<Receiver<BuildOutcome>>,
    build_running: bool,
    last_build_result: Option<bool>,
    analysis: TexAnalysis,
    active_math_preview: Option<MathPreview>,
    math_preview_texture: Option<TextureHandle>,
    math_preview_render_error: Option<String>,
    math_preview_render_key: Option<String>,
    math_preview_requested_key: Option<String>,
    math_preview_receiver: Option<Receiver<MathPreviewOutcome>>,
    math_preview_running: bool,
    math_preview_edit_deadline: Option<Instant>,
    last_cursor_index: Option<usize>,
    pdf_preview_textures: Vec<TextureHandle>,
    pdf_preview_render_error: Option<String>,
    pdf_preview_render_key: Option<String>,
    selected_pdf_path: Option<PathBuf>,
    template_entries: Vec<TemplateEntry>,
    selected_template_tex_path: Option<PathBuf>,
    template_copy_file_name: String,
    template_preview_textures: Vec<TextureHandle>,
    template_preview_render_error: Option<String>,
    template_preview_render_key: Option<String>,
    template_delete_confirm_path: Option<PathBuf>,
    texlab_sender: Option<Sender<TexlabCommand>>,
    texlab_receiver: Option<Receiver<TexlabEvent>>,
    texlab_status: Option<String>,
    completion_request_serial: u64,
    latest_completion_serial: u64,
    lsp_completion_items: Vec<LspCompletionItem>,
    completion_selected_index: usize,
    pending_cursor_jump: Option<usize>,
    pending_scroll_jump: Option<usize>,
    selected_tree_path: Option<PathBuf>,
    file_clipboard: Option<FileClipboard>,
    file_new_name: String,
    git_commit_message: String,
    external_tool_statuses: Vec<ExternalToolStatus>,
    confirm_close_requested: bool,
    allow_immediate_close: bool,
}

#[derive(Clone)]
struct FileClipboard {
    path: PathBuf,
    mode: FileClipboardMode,
}

#[derive(Clone)]
struct FileNode {
    path: PathBuf,
    is_dir: bool,
    children: Vec<FileNode>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum FileClipboardMode {
    Copy,
    Cut,
}

enum TreeAction {
    Delete(PathBuf),
    Copy(PathBuf),
    Cut(PathBuf),
    Paste(PathBuf),
    NewFile(PathBuf),
    NewFolder(PathBuf),
    AddToTemplates(PathBuf),
}

struct ExternalToolStatus {
    name: &'static str,
    path: Option<String>,
    detail: Option<String>,
}

#[derive(Clone)]
struct HunspellDictionary {
    name: String,
    path: PathBuf,
}

#[derive(Clone)]
struct TemplateEntry {
    tex_path: PathBuf,
    pdf_path: PathBuf,
    label: String,
}

#[derive(Clone)]
struct LspCompletionItem {
    label: String,
    insert_text: String,
    kind: Option<LspCompletionKind>,
    detail: Option<String>,
    deprecated: bool,
}

#[derive(Clone)]
struct EditorBuffer {
    text: String,
    last_cursor_index: Option<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LspCompletionKind {
    Text,
    Method,
    Function,
    Constructor,
    Field,
    Variable,
    Class,
    Interface,
    Module,
    Property,
    Unit,
    Value,
    Enum,
    Keyword,
    Snippet,
    Color,
    File,
    Reference,
    Folder,
    EnumMember,
    Constant,
    Struct,
    Event,
    Operator,
    TypeParameter,
}

enum TexlabCommand {
    SyncDocument {
        path: PathBuf,
        text: String,
    },
    RequestCompletion {
        path: PathBuf,
        text: String,
        cursor_char: usize,
        serial: u64,
    },
}

enum TexlabEvent {
    Ready,
    Completion {
        serial: u64,
        items: Vec<LspCompletionItem>,
    },
    Error(String),
}

impl TexEditorApp {
    fn save_settings(&self) {
        save_app_settings(&self.settings);
    }

    fn selected_dictionary_name(&self) -> Option<&str> {
        self.settings.preferred_dictionary.as_deref()
    }

    fn selected_hunspell_dictionary(&self) -> Option<&'static HunspellDictionary> {
        let preferred = self.selected_dictionary_name()?;
        available_hunspell_dictionaries()
            .iter()
            .find(|dictionary| dictionary.name == preferred)
    }

    fn spellcheck_status_label(&self) -> String {
        if !self.settings.spellcheck_enabled {
            return "disabled".to_owned();
        }
        let config = resolved_hunspell_config(self.selected_dictionary_name());
        if let Some(config) = config {
            return format!("{} via hunspell", config.dictionary_name);
        }
        if self
            .selected_dictionary_name()
            .map(|name| name.starts_with("en_"))
            .unwrap_or(true)
            && english_dictionary().is_some()
        {
            return self
                .selected_dictionary_name()
                .unwrap_or("en_US")
                .to_owned();
        }
        if let Some(name) = self.selected_dictionary_name() {
            return format!("{name} unavailable");
        }
        "no dictionary".to_owned()
    }

    fn apply_settings_change(&mut self, message: &str) {
        self.save_settings();
        self.refresh_analysis();
        self.refresh_external_tool_statuses();
        self.status_message = message.to_owned();
    }

    fn refresh_template_entries(&mut self) {
        ensure_default_templates_installed();
        self.template_entries = discover_template_entries();
        if self.template_entries.is_empty() {
            self.selected_template_tex_path = None;
            self.template_copy_file_name.clear();
            self.template_preview_textures.clear();
            self.template_preview_render_error = None;
            self.template_preview_render_key = None;
            return;
        }

        let still_exists = self
            .selected_template_tex_path
            .as_ref()
            .map(|selected| self.template_entries.iter().any(|entry| &entry.tex_path == selected))
            .unwrap_or(false);
        if !still_exists {
            self.selected_template_tex_path = self
                .template_entries
                .first()
                .map(|entry| entry.tex_path.clone());
            self.template_copy_file_name = self
                .template_entries
                .first()
                .map(|entry| template_default_copy_name(&entry.tex_path))
                .unwrap_or_default();
            self.template_preview_render_key = None;
            self.template_preview_render_error = None;
            self.template_preview_textures.clear();
        }
    }

    fn selected_template_entry(&self) -> Option<&TemplateEntry> {
        let selected = self.selected_template_tex_path.as_ref()?;
        self.template_entries
            .iter()
            .find(|entry| &entry.tex_path == selected)
    }

    fn copy_selected_template_to_workspace(&mut self) {
        let Some(entry) = self.selected_template_entry().cloned() else {
            self.status_message = "No template selected".to_owned();
            return;
        };

        let file_name = normalize_template_copy_name(&self.template_copy_file_name);
        let Some(file_name) = file_name else {
            self.status_message = "Template copy failed: file name is empty".to_owned();
            return;
        };

        let destination_tex = self.working_directory().join(&file_name);
        let destination_pdf = output_pdf_path(&destination_tex);
        if destination_tex.exists() || destination_pdf.exists() {
            self.status_message = format!(
                "Template copy failed: {} or its PDF already exists",
                destination_tex.display()
            );
            return;
        }

        let result = copy_path_recursively(&entry.tex_path, &destination_tex)
            .and_then(|_| copy_path_recursively(&entry.pdf_path, &destination_pdf));

        match result {
            Ok(()) => {
                self.mark_file_tree_dirty();
                self.selected_tree_path = Some(destination_tex.clone());
                self.status_message =
                    format!("Copied template into {}", self.working_directory().display());
            }
            Err(err) => {
                self.status_message = format!("Template copy failed: {err}");
            }
        }
    }

    fn request_delete_selected_template(&mut self) {
        let Some(entry) = self.selected_template_entry() else {
            self.status_message = "No template selected".to_owned();
            return;
        };
        self.template_delete_confirm_path = Some(entry.tex_path.clone());
    }

    fn delete_template(&mut self, tex_path: &Path) {
        let pdf_path = output_pdf_path(tex_path);
        let tex_result = remove_path_recursively(tex_path);
        if let Err(err) = tex_result {
            self.status_message = format!("Template delete failed: {err}");
            return;
        }
        if pdf_path.exists() {
            if let Err(err) = remove_path_recursively(&pdf_path) {
                self.status_message = format!("Template PDF delete failed: {err}");
                return;
            }
        }

        self.refresh_template_entries();
        self.template_preview_render_key = None;
        self.template_preview_render_error = None;
        self.template_preview_textures.clear();
        self.status_message = format!("Deleted template {}", display_name(tex_path));
    }

    fn text_differs_from_saved(&self, path: Option<&Path>, text: &str) -> bool {
        match path {
            Some(path) => fs::read_to_string(path)
                .map(|saved| saved != text)
                .unwrap_or(!text.trim().is_empty()),
            None => !text.trim().is_empty(),
        }
    }

    fn recompute_current_dirty(&mut self) {
        self.dirty = self.text_differs_from_saved(self.current_path.as_deref(), &self.text);
    }

    fn store_current_buffer(&mut self) {
        if let Some(path) = self.current_path.clone() {
            self.file_buffers.insert(
                path,
                EditorBuffer {
                    text: self.text.clone(),
                    last_cursor_index: self.last_cursor_index,
                },
            );
        }
    }

    fn file_has_unsaved_changes(&self, path: &Path) -> bool {
        if self.current_path.as_deref() == Some(path) {
            return self.text_differs_from_saved(Some(path), &self.text);
        }
        self.file_buffers
            .get(path)
            .map(|buffer| self.text_differs_from_saved(Some(path), &buffer.text))
            .unwrap_or(false)
    }

    fn has_unsaved_workspace_changes(&self) -> bool {
        let Some(root) = self.opened_directory.as_deref() else {
            return false;
        };

        (self.current_path.as_deref().map(|path| path.starts_with(root)).unwrap_or(false)
            && self.text_differs_from_saved(self.current_path.as_deref(), &self.text))
            || self
                .file_buffers
                .iter()
                .any(|(path, buffer)| {
                    path.starts_with(root) && self.text_differs_from_saved(Some(path), &buffer.text)
                })
    }

    fn handle_close_request(&mut self, ctx: &egui::Context) {
        if self.allow_immediate_close {
            return;
        }
        if ctx.input(|i| i.viewport().close_requested()) && self.has_unsaved_workspace_changes() {
            ctx.send_viewport_cmd(ViewportCommand::CancelClose);
            self.confirm_close_requested = true;
        }
    }

    fn show_close_confirm_dialog(&mut self, ctx: &egui::Context) {
        if !self.confirm_close_requested {
            return;
        }

        egui::Window::new("Unsaved Changes")
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
            .show(ctx, |ui| {
                ui.label("Save changes before closing?");
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Save").clicked() {
                        self.save_document();
                        if !self.has_unsaved_workspace_changes() {
                            self.confirm_close_requested = false;
                            ctx.send_viewport_cmd(ViewportCommand::Close);
                        }
                    }
                    if ui.button("Don't Save").clicked() {
                        self.confirm_close_requested = false;
                        self.allow_immediate_close = true;
                        ctx.send_viewport_cmd(ViewportCommand::Close);
                    }
                    if ui.button("Cancel").clicked() {
                        self.confirm_close_requested = false;
                    }
                });
            });
    }

    fn request_open_document_at_path(&mut self, path: PathBuf) {
        self.open_document_at_path(path);
    }

    fn request_open_pdf_at_path(&mut self, path: PathBuf) {
        self.open_pdf_at_path(path);
    }

    fn register_recent_directory(&mut self, path: &Path) {
        self.recent_directories.retain(|item| item != path);
        self.recent_directories.insert(0, path.to_path_buf());
        self.recent_directories.truncate(8);
        save_recent_directories(&self.recent_directories);
    }

    fn set_opened_directory(&mut self, path: PathBuf) {
        self.register_recent_directory(&path);
        self.opened_directory = Some(path.clone());
        self.selected_tree_path = Some(path.clone());
        self.file_tree = build_file_node(&path);
        self.file_tree_dirty = false;
        self.current_path = None;
        self.selected_pdf_path = None;
        self.active_left_tab = LeftPanelTab::File;
        self.refresh_git_status();
        self.refresh_external_tool_statuses();
        self.status_message = format!("Opened directory {}", path.display());
    }

    fn new_document(&mut self) {
        self.store_current_buffer();
        self.text.clear();
        self.current_path = None;
        self.recompute_current_dirty();
        self.status_message = "New TeX document".to_owned();
        self.refresh_analysis();
        self.refresh_git_status();
        self.active_math_preview = math_preview_at_cursor(&self.text, 0);
        self.sync_texlab_document();
        self.math_preview_requested_key = None;
        self.math_preview_receiver = None;
        self.math_preview_running = false;
        self.math_preview_edit_deadline = None;
        self.last_cursor_index = None;
        self.pdf_preview_render_key = None;
        self.selected_pdf_path = None;
    }

    fn refresh_file_tree(&mut self) {
        let root = self.working_directory();
        self.file_tree = build_file_node(&root);
        self.file_tree_dirty = false;
    }

    fn mark_file_tree_dirty(&mut self) {
        self.file_tree_dirty = true;
    }

    fn open_directory(&mut self) {
        let Some(path) = FileDialog::new()
            .set_directory(self.working_directory())
            .pick_folder()
        else {
            return;
        };

        self.set_opened_directory(path);
    }

    fn save_document(&mut self) {
        if let Some(path) = self.current_path.clone() {
            self.write_to_path(&path);
        } else {
            self.save_document_as();
        }
    }

    fn save_document_as(&mut self) {
        let dialog = FileDialog::new()
            .add_filter("TeX", &["tex"])
            .set_file_name(default_file_name(self.current_path.as_deref()))
            .set_directory(self.working_directory());
        let Some(path) = dialog
            .save_file()
        else {
            return;
        };

        self.write_to_path(&path);
    }

    fn write_to_path(&mut self, path: &Path) {
        match fs::write(path, &self.text) {
            Ok(()) => {
                if self.opened_directory.is_none() {
                    if let Some(parent) = path.parent() {
                        self.register_recent_directory(parent);
                    }
                    self.opened_directory = path.parent().map(Path::to_path_buf);
                }
                self.current_path = Some(path.to_path_buf());
                self.selected_tree_path = Some(path.to_path_buf());
                self.dirty = false;
                self.file_buffers.insert(
                    path.to_path_buf(),
                    EditorBuffer {
                        text: self.text.clone(),
                        last_cursor_index: self.last_cursor_index,
                    },
                );
                self.mark_file_tree_dirty();
                self.selected_pdf_path = None;
                self.refresh_git_status();
                self.status_message = format!("Saved {}", display_name(path));
            }
            Err(err) => {
                self.status_message = format!("Save failed: {err}");
            }
        }
    }

    fn refresh_analysis(&mut self) {
        self.analysis = analyze_tex(
            &self.text,
            self.last_cursor_index
                .map(|cursor| char_index_to_line(&self.text, cursor)),
            &self.settings,
        );
    }

    fn refresh_git_status(&mut self) {
        self.git_status = read_git_status(&self.working_directory());
    }

    fn refresh_external_tool_statuses(&mut self) {
        self.external_tool_statuses = vec![
            ExternalToolStatus {
                name: "latexmk",
                path: resolve_command_path("latexmk"),
                detail: None,
            },
            ExternalToolStatus {
                name: "lualatex",
                path: resolve_command_path("lualatex"),
                detail: None,
            },
            ExternalToolStatus {
                name: "pdftoppm",
                path: resolve_command_path("pdftoppm"),
                detail: None,
            },
            ExternalToolStatus {
                name: "texlab",
                path: resolve_command_path("texlab"),
                detail: self.texlab_status.clone(),
            },
            ExternalToolStatus {
                name: "hunspell",
                path: resolve_command_path("hunspell"),
                detail: Some(self.spellcheck_status_label()),
            },
            ExternalToolStatus {
                name: "git",
                path: resolve_command_path("git"),
                detail: None,
            },
            ExternalToolStatus {
                name: "gh",
                path: resolve_command_path("gh"),
                detail: None,
            },
        ];
    }

    fn sync_texlab_document(&mut self) {
        let Some(sender) = &self.texlab_sender else {
            return;
        };
        let path = self.texlab_document_path();
        let _ = sender.send(TexlabCommand::SyncDocument {
            path,
            text: self.text.clone(),
        });
    }

    fn request_texlab_completion(&mut self, cursor_char: usize) {
        let Some(sender) = &self.texlab_sender else {
            return;
        };
        let path = self.texlab_document_path();
        self.completion_request_serial += 1;
        self.latest_completion_serial = self.completion_request_serial;
        let _ = sender.send(TexlabCommand::RequestCompletion {
            path,
            text: self.text.clone(),
            cursor_char,
            serial: self.completion_request_serial,
        });
    }

    fn texlab_document_path(&self) -> PathBuf {
        self.current_path
            .clone()
            .unwrap_or_else(|| self.working_directory().join(".tex-editor-untitled.tex"))
    }

    fn apply_completion(&mut self, start_char: usize, end_char: usize, item: &str) {
        let (replace_start_char, replacement, replace_end_char, cursor_char) =
            completion_replacement(&self.text, start_char, end_char, item);
        replace_char_range(
            &mut self.text,
            replace_start_char,
            replace_end_char,
            &replacement,
        );
        self.recompute_current_dirty();
        self.status_message = format!("Completed {}", item);
        self.refresh_analysis();
        self.sync_texlab_document();
        self.last_cursor_index = Some(cursor_char);
        self.pending_cursor_jump = Some(cursor_char);
        self.pending_scroll_jump = Some(cursor_char);
        self.math_preview_edit_deadline = Some(Instant::now() + MATH_PREVIEW_IDLE_DELAY);
    }

    fn poll_texlab_events(&mut self) {
        loop {
            let event = match self.texlab_receiver.as_ref() {
                Some(receiver) => receiver.try_recv(),
                None => break,
            };

            match event {
                Ok(TexlabEvent::Ready) => {
                    self.texlab_status = Some("texlab ready".to_owned());
                    self.refresh_external_tool_statuses();
                    self.sync_texlab_document();
                }
                Ok(TexlabEvent::Completion { serial, items }) => {
                    if serial == self.latest_completion_serial {
                        self.lsp_completion_items = items;
                    }
                }
                Ok(TexlabEvent::Error(err)) => {
                    self.texlab_status = Some(err);
                    self.refresh_external_tool_statuses();
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    self.texlab_receiver = None;
                    self.texlab_sender = None;
                    self.texlab_status = Some("texlab disconnected".to_owned());
                    self.refresh_external_tool_statuses();
                    break;
                }
            }
        }
    }

    fn working_directory(&self) -> PathBuf {
        self.opened_directory
            .clone()
            .or_else(|| {
                self.current_path
                    .as_deref()
                    .and_then(Path::parent)
                    .map(Path::to_path_buf)
            })
            .or_else(|| std::env::current_dir().ok())
            .unwrap_or_else(|| PathBuf::from("."))
    }

    fn open_document_at_path(&mut self, path: PathBuf) {
        self.store_current_buffer();

        let buffer = if let Some(buffer) = self.file_buffers.get(&path).cloned() {
            buffer
        } else {
            match fs::read_to_string(&path) {
                Ok(text) => EditorBuffer {
                    text,
                    last_cursor_index: None,
                },
                Err(err) => {
                    self.status_message = format!("Open failed: {err}");
                    return;
                }
            }
        };

        self.text = buffer.text.clone();
        self.dirty = self.text_differs_from_saved(Some(&path), &self.text);
        self.last_cursor_index = buffer.last_cursor_index;
        self.file_buffers.insert(path.clone(), buffer);
        if self.opened_directory.is_none() {
            if let Some(parent) = path.parent() {
                self.register_recent_directory(parent);
            }
            self.opened_directory = path.parent().map(Path::to_path_buf);
        }
        self.current_path = Some(path.clone());
        self.selected_tree_path = Some(path.clone());
        self.selected_pdf_path = None;
        self.status_message = format!("Opened {}", display_name(&path));
        self.refresh_analysis();
        self.refresh_git_status();
        self.active_math_preview = math_preview_at_cursor(&self.text, self.last_cursor_index.unwrap_or(0));
        self.sync_texlab_document();
        self.math_preview_requested_key = None;
        self.math_preview_receiver = None;
        self.math_preview_running = false;
        self.math_preview_edit_deadline = None;
        self.pdf_preview_render_key = None;
    }

    fn open_pdf_at_path(&mut self, path: PathBuf) {
        if let Some(parent) = path.parent() {
            if self.opened_directory.is_none() {
                self.register_recent_directory(parent);
                self.opened_directory = Some(parent.to_path_buf());
            }
        }
        self.selected_tree_path = Some(path.clone());
        self.selected_pdf_path = Some(path.clone());
        self.active_center_tab = CenterPanelTab::Pdf;
        self.pdf_preview_render_key = None;
        self.pdf_preview_render_error = None;

        if let Some(tex_path) = corresponding_tex_path(&path) {
            self.open_document_at_path(tex_path);
            self.selected_pdf_path = Some(path.clone());
            self.active_center_tab = CenterPanelTab::Pdf;
        } else {
            self.status_message = format!("Opened {}", display_name(&path));
        }
    }

    fn showing_directory_placeholder(&self) -> bool {
        self.current_path.is_none() && !self.has_unsaved_workspace_changes() && self.text.trim().is_empty()
    }

    fn build_document(&mut self) {
        if self.build_running {
            return;
        }

        let Some(path) = self.current_path.clone() else {
            self.status_message = "Save the file before build".to_owned();
            self.build_log = "No target file.\n".to_owned();
            return;
        };

        if self.dirty {
            self.write_to_path(&path);
            if self.dirty {
                self.build_log = "Build aborted because the current document could not be saved.\n".to_owned();
                return;
            }
        }

        let project_root = path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| self.working_directory());

        let Some(tool) = resolve_preferred_build_tool(self.settings.build_tool) else {
            self.status_message = "No TeX build tool found".to_owned();
            self.build_log =
                "Install `latexmk` or `tectonic`, or place them at a known path, to build from this editor.\n"
                    .to_owned();
            return;
        };
        let args = if tool.kind == BuildToolPreference::Latexmk {
            match latexmk_build_args(&path, &project_root) {
                Ok(args) => args,
                Err(err) => {
                    self.status_message = format!("Build failed: {err}");
                    self.build_log = format!("{err}\n");
                    return;
                }
            }
        } else {
            vec![file_name_for_build(&path)]
        };

        self.build_tool = Some(tool.path.clone());
        self.status_message = format!("Building with {}", display_name(Path::new(&tool.path)));
        self.build_running = true;
        self.last_build_result = None;
        self.build_log = format!("$ {} {}\n\nBuilding...\n", tool.path, args.join(" "));

        let (sender, receiver) = mpsc::channel();
        self.build_receiver = Some(receiver);

        let build_root = project_root.clone();
        let path_for_result = path.clone();
        std::thread::spawn(move || {
            let outcome = run_build_command(tool.path, args, build_root, path_for_result);
            let _ = sender.send(outcome);
        });
    }

    fn poll_build_status(&mut self) {
        let Some(receiver) = &self.build_receiver else {
            return;
        };

        match receiver.try_recv() {
            Ok(outcome) => {
                self.build_running = false;
                self.build_receiver = None;
                self.build_log = outcome.log;
                self.status_message = outcome.status_message;
                self.last_build_result = Some(outcome.success);
                if outcome.success {
                    self.pdf_preview_render_key = None;
                    self.selected_pdf_path = outcome.pdf_path.clone();
                    if self.settings.open_pdf_after_build && outcome.pdf_path.is_some() {
                        self.active_center_tab = CenterPanelTab::Pdf;
                    }
                }
            }
            Err(TryRecvError::Empty) => {}
            Err(TryRecvError::Disconnected) => {
                self.build_running = false;
                self.build_receiver = None;
                self.last_build_result = Some(false);
                self.status_message = "Build failed: background task disconnected".to_owned();
            }
        }
    }

    fn handle_shortcuts(&mut self, ctx: &egui::Context) {
        if consume_shortcut(ctx, Key::S) {
            self.save_document();
        }
        if consume_shortcut(ctx, Key::B) {
            self.build_document();
        }
    }

    fn window_title(&self) -> String {
        let base = self
            .current_path
            .as_deref()
            .map(display_name)
            .unwrap_or_else(|| "untitled.tex".to_owned());

        if self.has_unsaved_workspace_changes() {
            format!("camellia-editor - {base} *")
        } else {
            format!("camellia-editor - {base}")
        }
    }

    fn show_toolbar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::top("toolbar").show(ctx, |ui| {
            ui.horizontal_wrapped(|ui| {
                let folder_button = if self.opened_directory.is_some() {
                    "Change Folder"
                } else {
                    "Open Folder"
                };
                if ui.button(folder_button).clicked() {
                    self.open_directory();
                }
                ui.add_enabled_ui(!self.recent_directories.is_empty(), |ui| {
                    ui.menu_button("Recent Folders", |ui| {
                        let recent_directories = self.recent_directories.clone();
                        for path in recent_directories {
                            if ui.button(path.display().to_string()).clicked() {
                                self.set_opened_directory(path);
                                ui.close_menu();
                            }
                        }
                    });
                });
                if ui.button("Refresh Tools").clicked() {
                    self.refresh_external_tool_statuses();
                }
                if ui.button("Settings").clicked() {
                    self.show_settings_window = true;
                }
                if ui.button("Templates").clicked() {
                    self.refresh_template_entries();
                    self.show_templates_window = true;
                }
                if ui.button("Save").clicked() {
                    self.save_document();
                }
                if ui.button("Save As").clicked() {
                    self.save_document_as();
                }
                if ui
                    .add_enabled(!self.build_running, egui::Button::new("Build"))
                    .clicked()
                {
                    self.build_document();
                }
                if self.build_running {
                    ui.spinner();
                } else if let Some(success) = self.last_build_result {
                    ui.label(if success { "✅" } else { "❌" });
                }

                ui.separator();
                ui.label(
                    self.current_path
                        .as_deref()
                        .map(display_name)
                        .unwrap_or_else(|| "No file selected".to_owned()),
                );
                ui.separator();
                let (save_text, save_color) = if self.has_unsaved_workspace_changes() {
                    ("unsaved", Color32::from_rgb(220, 170, 60))
                } else {
                    ("saved", Color32::from_rgb(90, 190, 120))
                };
                ui.label(RichText::new(save_text).color(save_color).strong());
            });
        });
    }

    fn show_settings_window(&mut self, ctx: &egui::Context) {
        if !self.show_settings_window {
            return;
        }

        let mut open = self.show_settings_window;
        egui::Window::new("Settings")
            .open(&mut open)
            .default_width(420.0)
            .resizable(true)
            .show(ctx, |ui| {
                ui.heading("Spell Check");
                ui.add_space(6.0);

                let mut spellcheck_enabled = self.settings.spellcheck_enabled;
                if ui.checkbox(&mut spellcheck_enabled, "Enable spell check").changed() {
                    self.settings.spellcheck_enabled = spellcheck_enabled;
                    self.apply_settings_change("Updated spell check setting");
                }

                let available_dictionaries = available_hunspell_dictionaries();
                let current_dictionary = self
                    .settings
                    .preferred_dictionary
                    .clone()
                    .unwrap_or_else(|| "Auto".to_owned());

                egui::ComboBox::from_label("Dictionary language")
                    .selected_text(dictionary_display_label(&current_dictionary))
                    .show_ui(ui, |ui| {
                        if ui
                            .selectable_label(self.settings.preferred_dictionary.is_none(), "Auto")
                            .clicked()
                        {
                            self.settings.preferred_dictionary = None;
                            self.apply_settings_change("Using automatic dictionary selection");
                        }

                        for dictionary in available_dictionaries {
                            let selected = self.settings.preferred_dictionary.as_deref()
                                == Some(dictionary.name.as_str());
                            if ui
                                .selectable_label(selected, dictionary_display_label(&dictionary.name))
                                .clicked()
                            {
                                self.settings.preferred_dictionary = Some(dictionary.name.clone());
                                self.apply_settings_change("Updated spell check dictionary");
                            }
                        }
                    });

                ui.small(format!("Current backend: {}", self.spellcheck_status_label()));
                if let Some(dictionary) = self.selected_hunspell_dictionary() {
                    ui.small(format!("Dictionary file: {}", dictionary.path.display()));
                } else if available_dictionaries.is_empty() {
                    ui.small("No Hunspell dictionaries were found. English fallback stays available when possible.");
                }

                ui.separator();
                ui.heading("Build");
                ui.add_space(6.0);

                egui::ComboBox::from_label("Preferred build tool")
                    .selected_text(self.settings.build_tool.label())
                    .show_ui(ui, |ui| {
                        for option in [
                            BuildToolPreference::Auto,
                            BuildToolPreference::Latexmk,
                            BuildToolPreference::Tectonic,
                        ] {
                            if ui
                                .selectable_label(self.settings.build_tool == option, option.label())
                                .clicked()
                            {
                                self.settings.build_tool = option;
                                self.apply_settings_change("Updated build tool preference");
                            }
                        }
                    });

                let mut open_pdf_after_build = self.settings.open_pdf_after_build;
                if ui
                    .checkbox(
                        &mut open_pdf_after_build,
                        "Switch to PDF preview automatically after successful build",
                    )
                    .changed()
                {
                    self.settings.open_pdf_after_build = open_pdf_after_build;
                    self.apply_settings_change("Updated build preview setting");
                }

                ui.separator();
                ui.heading("Environment");
                ui.add_space(6.0);
                ui.label(format!(
                    "Detected dictionaries: {}",
                    available_dictionaries.len()
                ));
                ui.label(format!(
                    "Detected build tools: {}",
                    describe_detected_build_tools()
                ));
            });
        self.show_settings_window = open;
    }

    fn sync_template_preview_render(&mut self, ctx: &egui::Context) {
        let Some(entry) = self.selected_template_entry().cloned() else {
            self.template_preview_textures.clear();
            self.template_preview_render_error = None;
            self.template_preview_render_key = None;
            return;
        };
        if !entry.pdf_path.exists() {
            self.template_preview_textures.clear();
            self.template_preview_render_error =
                Some(format!("Missing PDF: {}", entry.pdf_path.display()));
            self.template_preview_render_key = None;
            return;
        }

        let render_key = pdf_render_cache_key(&entry.pdf_path);
        if self.template_preview_render_key.as_deref() == Some(render_key.as_str()) {
            return;
        }

        self.template_preview_render_key = Some(render_key);
        match render_pdf_preview_images(&entry.pdf_path) {
            Ok(images) => {
                self.template_preview_textures = images
                    .into_iter()
                    .enumerate()
                    .map(|(index, image)| {
                        ctx.load_texture(
                            format!("template_pdf_preview_texture_{index}"),
                            image,
                            TextureOptions::LINEAR,
                        )
                    })
                    .collect();
                self.template_preview_render_error = None;
            }
            Err(err) => {
                self.template_preview_textures.clear();
                self.template_preview_render_error = Some(err);
            }
        }
    }

    fn show_templates_window(&mut self, ctx: &egui::Context) {
        if !self.show_templates_window {
            return;
        }

        let modal_open = self.template_delete_confirm_path.is_some();
        let mut open = self.show_templates_window;
        egui::Window::new("Templates")
            .open(&mut open)
            .default_size(egui::vec2(960.0, 680.0))
            .show(ctx, |ui| {
                ui.add_enabled_ui(!modal_open, |ui| {
                    ui.horizontal(|ui| {
                        ui.heading("Template Browser");
                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                            if ui.button("Refresh").clicked() {
                                self.refresh_template_entries();
                                self.status_message = "Refreshed templates".to_owned();
                            }
                        });
                    });
                    ui.add_space(6.0);

                    if self.template_entries.is_empty() {
                        ui.label("No templates found under the installed templates folder.");
                        ui.small("Expected layout: templates/*.tex and templates/**/out/*.pdf");
                        return;
                    }

                    self.sync_template_preview_render(ctx);

                    ui.columns(2, |columns| {
                        columns[0].set_min_width(260.0);
                        columns[0].label(format!("{} template(s)", self.template_entries.len()));
                        columns[0].add_space(6.0);
                        ScrollArea::vertical()
                            .id_salt("templates_list_scroll")
                            .show(&mut columns[0], |ui| {
                                for entry in self.template_entries.clone() {
                                    let selected = self.selected_template_tex_path.as_deref()
                                        == Some(entry.tex_path.as_path());
                                    ui.horizontal(|ui| {
                                        let response = ui.selectable_label(selected, &entry.label);
                                        if response.clicked() {
                                            self.selected_template_tex_path = Some(entry.tex_path.clone());
                                            self.template_copy_file_name =
                                                template_default_copy_name(&entry.tex_path);
                                            self.template_preview_render_key = None;
                                            self.template_preview_render_error = None;
                                        }
                                        ui.with_layout(Layout::right_to_left(Align::Center), |ui| {
                                            if ui.small_button("🗑").clicked() {
                                                self.selected_template_tex_path = Some(entry.tex_path.clone());
                                                self.request_delete_selected_template();
                                            }
                                        });
                                    });
                                }
                            });

                        if let Some(entry) = self.selected_template_entry() {
                            columns[1].label(RichText::new(&entry.label).strong());
                            columns[1].small(format!("TeX: {}", entry.tex_path.display()));
                            columns[1].small(format!("PDF: {}", entry.pdf_path.display()));
                            columns[1].add_space(8.0);
                            columns[1].horizontal(|ui| {
                                ui.label("File name");
                                ui.add(
                                    egui::TextEdit::singleline(&mut self.template_copy_file_name)
                                        .desired_width(220.0),
                                );
                                let can_copy = !self.template_copy_file_name.trim().is_empty();
                                if ui
                                    .add_enabled(can_copy, egui::Button::new("Copy To Current Folder"))
                                    .clicked()
                                {
                                    self.copy_selected_template_to_workspace();
                                }
                            });
                            columns[1].small(format!(
                                "Destination: {}",
                                self.working_directory().display()
                            ));
                            columns[1].add_space(8.0);
                            ScrollArea::vertical()
                                .id_salt("template_pdf_preview_scroll")
                                .show(&mut columns[1], |ui| {
                                    if !self.template_preview_textures.is_empty() {
                                        ui.vertical_centered(|ui| {
                                            for texture in &self.template_preview_textures {
                                                let available_width = ui.available_width().max(1.0);
                                                let texture_size = texture.size_vec2();
                                                let scale = (available_width / texture_size.x).min(1.0);
                                                let size = texture_size * scale;
                                                ui.add(
                                                    egui::Image::from_texture(texture)
                                                        .fit_to_exact_size(size),
                                                );
                                                ui.add_space(12.0);
                                            }
                                        });
                                    } else if let Some(error) = &self.template_preview_render_error {
                                        ui.colored_label(Color32::from_rgb(220, 120, 120), error);
                                    } else {
                                        ui.label("PDF preview is not available yet.");
                                    }
                                });
                        }
                    });
                });
            });
        self.show_templates_window = open;
    }

    fn show_template_delete_confirm_dialog(&mut self, ctx: &egui::Context) {
        let Some(tex_path) = self.template_delete_confirm_path.clone() else {
            return;
        };

        egui::Window::new("Delete Template")
            .order(egui::Order::Foreground)
            .collapsible(false)
            .resizable(false)
            .anchor(egui::Align2::CENTER_CENTER, egui::vec2(0.0, 0.0))
            .show(ctx, |ui| {
                ui.label("Delete this template?");
                ui.small(display_name(&tex_path));
                ui.add_space(8.0);
                ui.horizontal(|ui| {
                    if ui.button("Delete").clicked() {
                        self.template_delete_confirm_path = None;
                        self.delete_template(&tex_path);
                    }
                    if ui.button("Cancel").clicked() {
                        self.template_delete_confirm_path = None;
                    }
                });
            });
    }

    fn show_math_preview(&mut self, ui: &mut egui::Ui) {
        ui.horizontal_wrapped(|ui| {
            ui.heading("Math Preview");
            if let Some(preview) = &self.active_math_preview {
                ui.separator();
                ui.label(format!("mode: {}", preview.mode));
                ui.separator();
                ui.label(format!("line {}", preview.line));
            } else {
                ui.separator();
                ui.label("standby");
            }
        });
        ui.add_space(6.0);
        ui.group(|ui| {
            ui.set_min_height(110.0);
            ScrollArea::vertical()
                .max_height(145.0)
                .show(ui, |ui| {
                    if let Some(texture) = &self.math_preview_texture {
                        ui.vertical_centered(|ui| {
                            let available_width = ui.available_width().max(1.0);
                            let texture_size = texture.size_vec2();
                            let scale = (available_width / texture_size.x).min(1.0);
                            let size = texture_size * scale;
                            ui.add(egui::Image::from_texture(texture).fit_to_exact_size(size));
                            if self.math_preview_running {
                                ui.add_space(8.0);
                                ui.spinner();
                            }
                        });
                    } else if self.math_preview_running {
                        ui.vertical_centered(|ui| {
                            ui.add_space(16.0);
                            ui.spinner();
                        });
                    } else if let Some(error) = &self.math_preview_render_error {
                        ui.label(RichText::new(error).color(Color32::from_rgb(220, 120, 120)));
                    } else if let Some(preview) = &self.active_math_preview {
                        ui.label(
                            RichText::new(&preview.source)
                                .monospace()
                                .size(20.0)
                                .color(Color32::from_rgb(230, 190, 120)),
                        );
                    } else {
                        ui.label(
                            RichText::new("Move the cursor onto a math expression to preview it.")
                                .italics()
                                .color(Color32::from_gray(150)),
                        );
                    }
                });
        });
    }

    fn sync_math_preview_render(&mut self, ctx: &egui::Context) {
        let Some(preview) = &self.active_math_preview else {
            self.math_preview_texture = None;
            self.math_preview_render_error = None;
            self.math_preview_render_key = None;
            self.math_preview_requested_key = None;
            self.math_preview_receiver = None;
            self.math_preview_running = false;
            self.math_preview_edit_deadline = None;
            return;
        };

        if let Some(deadline) = self.math_preview_edit_deadline {
            let now = Instant::now();
            if now < deadline {
                ctx.request_repaint_after(deadline.saturating_duration_since(now));
                return;
            }
            self.math_preview_edit_deadline = None;
        }

        let preamble = extract_preview_preamble(&self.text);
        let render_key = format!("{}\n{}\n{}", preview.mode, preview.source, preamble);
        if self.math_preview_render_key.as_deref() == Some(render_key.as_str()) {
            return;
        }
        if self.math_preview_requested_key.as_deref() == Some(render_key.as_str()) {
            if self.math_preview_running {
                ctx.request_repaint_after(Duration::from_millis(50));
            }
            return;
        }

        self.start_math_preview_render(render_key, preview.clone(), preamble);
        ctx.request_repaint_after(Duration::from_millis(50));
    }

    fn start_math_preview_render(
        &mut self,
        render_key: String,
        preview: MathPreview,
        preamble: String,
    ) {
        let (sender, receiver) = mpsc::channel();
        self.math_preview_requested_key = Some(render_key.clone());
        self.math_preview_receiver = Some(receiver);
        self.math_preview_running = true;

        std::thread::spawn(move || {
            let image = render_math_preview_image(&preview, &preamble);
            let _ = sender.send(MathPreviewOutcome { render_key, image });
        });
    }

    fn poll_math_preview_status(&mut self, ctx: &egui::Context) {
        let Some(receiver) = &self.math_preview_receiver else {
            return;
        };

        match receiver.try_recv() {
            Ok(outcome) => {
                self.math_preview_running = false;
                self.math_preview_receiver = None;

                if self.math_preview_requested_key.as_deref() != Some(outcome.render_key.as_str()) {
                    return;
                }

                match outcome.image {
                    Ok(image) => {
                        self.math_preview_texture = Some(
                            ctx.load_texture("math_preview_texture", image, TextureOptions::LINEAR),
                        );
                        self.math_preview_render_key = Some(outcome.render_key);
                        self.math_preview_render_error = None;
                    }
                    Err(err) => {
                        if should_surface_math_preview_error(&err) {
                            self.math_preview_texture = None;
                            self.math_preview_render_error = Some(err);
                        } else {
                            self.math_preview_render_error = None;
                        }
                    }
                }
            }
            Err(TryRecvError::Empty) => {
                ctx.request_repaint_after(Duration::from_millis(50));
            }
            Err(TryRecvError::Disconnected) => {
                self.math_preview_running = false;
                self.math_preview_receiver = None;
                self.math_preview_render_error =
                    Some("Math preview task disconnected".to_owned());
            }
        }
    }

    fn sync_pdf_preview_render(&mut self, ctx: &egui::Context) {
        let Some(pdf_path) = current_pdf_preview_path(
            self.current_path.as_deref(),
            self.selected_pdf_path.as_deref(),
        ) else {
            self.pdf_preview_textures.clear();
            self.pdf_preview_render_error = None;
            self.pdf_preview_render_key = None;
            return;
        };

        let render_key = pdf_render_cache_key(&pdf_path);
        if self.pdf_preview_render_key.as_deref() == Some(render_key.as_str()) {
            return;
        }

        self.pdf_preview_render_key = Some(render_key.clone());

        match render_pdf_preview_images(&pdf_path) {
            Ok(images) => {
                self.pdf_preview_textures = images
                    .into_iter()
                    .enumerate()
                    .map(|(index, image)| {
                        ctx.load_texture(
                            format!("pdf_preview_texture_{index}"),
                            image,
                            TextureOptions::LINEAR,
                        )
                    })
                    .collect();
                self.pdf_preview_render_error = None;
            }
            Err(err) => {
                self.pdf_preview_textures.clear();
                self.pdf_preview_render_error = Some(err);
            }
        }
    }

    fn show_workspace_panel(&mut self, ctx: &egui::Context) {
        let mut selected_file = None;
        let mut tree_action = None;
        egui::SidePanel::left("workspace_panel")
            .resizable(true)
            .default_width(260.0)
            .min_width(180.0)
            .show(ctx, |ui| {
                ui.heading("Workspace");
                let working_directory = self.working_directory();
                ui.label(display_name(&working_directory));
                ui.add_space(6.0);
                ui.horizontal(|ui| {
                    ui.selectable_value(&mut self.active_left_tab, LeftPanelTab::Tex, "tex");
                    ui.selectable_value(&mut self.active_left_tab, LeftPanelTab::File, "file");
                    ui.selectable_value(&mut self.active_left_tab, LeftPanelTab::Git, "git");
                });
                ui.separator();

                match self.active_left_tab {
                    LeftPanelTab::Tex => self.show_tex_tab(ui),
                    LeftPanelTab::File => {
                        self.show_file_tab(ui, &mut selected_file, &mut tree_action);
                    }
                    LeftPanelTab::Git => self.show_git_tab(ui),
                }
            });

        if let Some(path) = selected_file {
            self.request_open_document_at_path(path);
        }
        if let Some(action) = tree_action {
            self.handle_tree_action(action);
        }
    }

    fn show_file_tab(
        &mut self,
        ui: &mut egui::Ui,
        selected_file: &mut Option<PathBuf>,
        tree_action: &mut Option<TreeAction>,
    ) {
        if self.opened_directory.is_none() {
            ui.label(RichText::new("Open a directory to show files").italics());
            return;
        }

        if self.file_tree_dirty {
            self.refresh_file_tree();
        }

        let working_directory = self.working_directory();
        ui.horizontal(|ui| {
            let root_response = ui
                .add(
                    egui::Label::new(
                        RichText::new(format!("📁 {}", working_directory.display()))
                            .strong()
                            .monospace(),
                    )
                    .sense(egui::Sense::click()),
                )
                .on_hover_text("Workspace root");
            if root_response.clicked() {
                self.selected_tree_path = Some(working_directory.clone());
            }
            root_response.context_menu(|ui| {
                show_tree_context_menu(
                    ui,
                    &working_directory,
                    &mut self.file_new_name,
                    self.file_clipboard.is_some(),
                    tree_action,
                    true,
                );
            });
            ui.with_layout(egui::Layout::right_to_left(Align::Center), |ui| {
                if ui.button("Refresh").clicked() {
                    self.refresh_file_tree();
                    self.status_message = "File tree refreshed".to_owned();
                }
            });
        });
        ui.separator();

        ScrollArea::vertical()
            .id_salt("workspace_tree_scroll")
            .show(ui, |ui| {
                let tree = self.file_tree.clone();
                self.render_file_node(ui, &tree, selected_file, tree_action);
            });
    }

    fn handle_tree_action(&mut self, action: TreeAction) {
        match action {
            TreeAction::Delete(path) => self.delete_path(&path),
            TreeAction::Copy(path) => {
                self.file_clipboard = Some(FileClipboard {
                    path: path.clone(),
                    mode: FileClipboardMode::Copy,
                });
                self.status_message = format!("Copied {}", display_name(&path));
            }
            TreeAction::Cut(path) => {
                self.file_clipboard = Some(FileClipboard {
                    path: path.clone(),
                    mode: FileClipboardMode::Cut,
                });
                self.status_message = format!("Cut {}", display_name(&path));
            }
            TreeAction::Paste(path) => self.paste_into(&path),
            TreeAction::NewFile(dir) => self.create_tree_entry(&dir, false),
            TreeAction::NewFolder(dir) => self.create_tree_entry(&dir, true),
            TreeAction::AddToTemplates(path) => self.add_to_templates(&path),
        }
        self.refresh_git_status();
    }

    fn render_file_node(
        &mut self,
        ui: &mut egui::Ui,
        node: &FileNode,
        selected_file: &mut Option<PathBuf>,
        tree_action: &mut Option<TreeAction>,
    ) {
        if node.is_dir {
            let header = egui::CollapsingHeader::new(
                RichText::new(format!("{} {}", tree_item_icon(&node.path, true), display_name(&node.path)))
                    .strong(),
            )
            .id_salt(&node.path)
            .default_open(self.opened_directory.as_deref() == Some(node.path.as_path()));

            let header_output = header.show(ui, |ui| {
                for child in &node.children {
                    self.render_file_node(ui, child, selected_file, tree_action);
                }
            });

            let response = header_output.header_response;
            if response.clicked() {
                self.selected_tree_path = Some(node.path.clone());
            }
            response.context_menu(|ui| {
                show_tree_context_menu(
                    ui,
                    &node.path,
                    &mut self.file_new_name,
                    self.file_clipboard.is_some(),
                    tree_action,
                    true,
                );
                if ui.button("copy").clicked() {
                    *tree_action = Some(TreeAction::Copy(node.path.clone()));
                    ui.close_menu();
                }
                if ui.button("cut").clicked() {
                    *tree_action = Some(TreeAction::Cut(node.path.clone()));
                    ui.close_menu();
                }
                if ui.button("delete").clicked() {
                    *tree_action = Some(TreeAction::Delete(node.path.clone()));
                    ui.close_menu();
                }
            });
        } else {
            let is_current = self.current_path.as_deref() == Some(node.path.as_path());
            let is_selected = self.selected_tree_path.as_deref() == Some(node.path.as_path());
            let is_unsaved = self.file_has_unsaved_changes(&node.path);
            let file_name = display_name(&node.path);
            let label_text = if is_unsaved {
                format!("{} {} •", tree_item_icon(&node.path, false), file_name)
            } else {
                format!("{} {}", tree_item_icon(&node.path, false), file_name)
            };
            let label = if is_current {
                RichText::new(label_text)
                    .strong()
                    .color(if is_unsaved {
                        Color32::from_rgb(255, 196, 122)
                    } else {
                        ui.visuals().strong_text_color()
                    })
            } else {
                RichText::new(label_text).color(if is_unsaved {
                    Color32::from_rgb(255, 196, 122)
                } else {
                    ui.visuals().text_color()
                })
            };
            let response = ui.selectable_label(is_current || is_selected, label);
            if response.clicked() {
                self.selected_tree_path = Some(node.path.clone());
                if node
                    .path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("pdf"))
                    .unwrap_or(false)
                {
                    self.request_open_pdf_at_path(node.path.clone());
                } else if is_editor_text_path(&node.path) {
                    *selected_file = Some(node.path.clone());
                }
            }
            response.context_menu(|ui| {
                let parent = node
                    .path
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| node.path.clone());
                if ui.button("paste").clicked() {
                    *tree_action = Some(TreeAction::Paste(parent.clone()));
                    ui.close_menu();
                }
                if ui.button("copy").clicked() {
                    *tree_action = Some(TreeAction::Copy(node.path.clone()));
                    ui.close_menu();
                }
                if ui.button("cut").clicked() {
                    *tree_action = Some(TreeAction::Cut(node.path.clone()));
                    ui.close_menu();
                }
                if ui.button("delete").clicked() {
                    *tree_action = Some(TreeAction::Delete(node.path.clone()));
                    ui.close_menu();
                }
                if node
                    .path
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("tex"))
                    .unwrap_or(false)
                {
                    ui.separator();
                    if ui.button("add to templates").clicked() {
                        *tree_action = Some(TreeAction::AddToTemplates(node.path.clone()));
                        ui.close_menu();
                    }
                }
            });
        }
    }

    fn create_tree_entry(&mut self, dir: &Path, is_dir: bool) {
        let name = self.file_new_name.trim();
        if name.is_empty() {
            self.status_message = "Name is empty".to_owned();
            return;
        }
        let path = dir.join(name);
        let result = if is_dir {
            fs::create_dir_all(&path)
        } else {
            fs::write(&path, "")
        };
        match result {
            Ok(()) => {
                self.selected_tree_path = Some(path.clone());
                self.mark_file_tree_dirty();
                self.status_message = format!(
                    "{} {}",
                    if is_dir { "Created folder" } else { "Created file" },
                    display_name(&path)
                );
                if !is_dir && is_tex_path(&path) {
                    self.open_document_at_path(path);
                }
                self.file_new_name.clear();
            }
            Err(err) => {
                self.status_message = format!("Create failed: {err}");
            }
        }
    }

    fn add_to_templates(&mut self, tex_path: &Path) {
        let source_pdf = output_pdf_path(tex_path);
        if !source_pdf.exists() {
            self.status_message = format!(
                "Template add failed: corresponding PDF was not found at {}",
                source_pdf.display()
            );
            return;
        }

        let templates_root = primary_template_root();
        let target_tex_name = tex_path
            .file_name()
            .unwrap_or_else(|| std::ffi::OsStr::new("template.tex"));
        let target_tex_path = unique_destination_path(&templates_root, target_tex_name);
        let target_pdf_path = output_pdf_path(&target_tex_path);

        let result = copy_path_recursively(tex_path, &target_tex_path)
            .and_then(|_| copy_path_recursively(&source_pdf, &target_pdf_path));

        match result {
            Ok(()) => {
                self.refresh_template_entries();
                self.selected_template_tex_path = Some(target_tex_path.clone());
                self.template_preview_render_key = None;
                self.template_preview_render_error = None;
                self.template_preview_textures.clear();
                self.show_templates_window = true;
                self.status_message =
                    format!("Added template {}", display_name(&target_tex_path));
            }
            Err(err) => {
                self.status_message = format!("Template add failed: {err}");
            }
        }
    }

    fn paste_into(&mut self, dir: &Path) {
        let Some(clipboard) = self.file_clipboard.clone() else {
            return;
        };
        let Some(file_name) = clipboard.path.file_name() else {
            self.status_message = "Paste failed: invalid source".to_owned();
            return;
        };
        let destination = unique_destination_path(dir, file_name);
        let result = match clipboard.mode {
            FileClipboardMode::Copy => copy_path_recursively(&clipboard.path, &destination),
            FileClipboardMode::Cut => fs::rename(&clipboard.path, &destination)
                .or_else(|_| copy_path_recursively(&clipboard.path, &destination).and_then(|_| remove_path_recursively(&clipboard.path))),
        };
        match result {
            Ok(()) => {
                self.status_message = format!("Pasted {}", display_name(&destination));
                self.selected_tree_path = Some(destination.clone());
                self.mark_file_tree_dirty();
                if clipboard.mode == FileClipboardMode::Cut {
                    self.file_clipboard = None;
                    if self.current_path.as_deref() == Some(clipboard.path.as_path()) {
                        self.current_path = Some(destination);
                    }
                }
            }
            Err(err) => {
                self.status_message = format!("Paste failed: {err}");
            }
        }
    }

    fn delete_path(&mut self, path: &Path) {
        match remove_path_recursively(path) {
            Ok(()) => {
                if self.current_path.as_deref() == Some(path) {
                    self.new_document();
                }
                if self.selected_tree_path.as_deref() == Some(path) {
                    self.selected_tree_path = None;
                }
                self.mark_file_tree_dirty();
                self.status_message = format!("Deleted {}", display_name(path));
            }
            Err(err) => {
                self.status_message = format!("Delete failed: {err}");
            }
        }
    }

    fn show_tex_tab(&mut self, ui: &mut egui::Ui) {
        ui.label(format!("{} sections", self.analysis.outline.len()));
        ui.add_space(6.0);

        ScrollArea::vertical()
            .id_salt("outline_sections_scroll")
            .show(ui, |ui| {
                if self.analysis.outline.is_empty() {
                    ui.label(RichText::new("No section commands found").italics());
                }

                for item in &self.analysis.outline {
                    ui.horizontal(|ui| {
                        ui.add_space((item.level.saturating_sub(1) as f32) * 12.0);
                        ui.vertical(|ui| {
                            let target = line_start_char(&self.text, item.line);
                            let command = egui::Button::new(RichText::new(&item.command).strong())
                                .fill(Color32::TRANSPARENT)
                                .stroke(egui::Stroke::NONE);
                            if ui.add(command).clicked() {
                                self.pending_cursor_jump = Some(target);
                                self.pending_scroll_jump = Some(target);
                                self.active_center_tab = CenterPanelTab::Tex;
                            }
                            let summary = egui::Button::new(format!("line {}: {}", item.line, item.title))
                                .fill(Color32::TRANSPARENT)
                                .stroke(egui::Stroke::NONE);
                            if ui.add(summary).clicked() {
                                self.pending_cursor_jump = Some(target);
                                self.pending_scroll_jump = Some(target);
                                self.active_center_tab = CenterPanelTab::Tex;
                            }
                        });
                    });
                    ui.add_space(6.0);
                }

                ui.separator();
                ui.heading("Symbols");
                ui.add_space(6.0);

                ScrollArea::vertical()
                    .id_salt("outline_symbols_scroll")
                    .max_height(220.0)
                    .show(ui, |ui| {
                        if self.analysis.symbols.is_empty() {
                            ui.label(RichText::new("No labels or refs").italics());
                        }

                        for symbol in &self.analysis.symbols {
                            ui.horizontal_wrapped(|ui| {
                                ui.label(RichText::new(&symbol.kind).monospace());
                                ui.label(&symbol.name);
                                ui.small(format!("line {}", symbol.line));
                            });
                        }
                    });
            });
    }

    fn show_git_tab(&mut self, ui: &mut egui::Ui) {
        let git_available = resolve_command_path("git").is_some();
        let gh_available = resolve_command_path("gh").is_some();

        ui.horizontal(|ui| {
            ui.label("Git Status");
            if ui.button("Refresh").clicked() {
                self.refresh_git_status();
                self.refresh_external_tool_statuses();
            }
        });
        ui.add_space(6.0);
        ui.horizontal_wrapped(|ui| {
            if ui
                .add_enabled(git_available, egui::Button::new("Init Repo"))
                .clicked()
            {
                self.run_git_init();
            }
            if ui
                .add_enabled(git_available, egui::Button::new("Commit All"))
                .clicked()
            {
                self.run_git_commit_all();
            }
            if ui
                .add_enabled(git_available, egui::Button::new("Push"))
                .clicked()
            {
                self.run_git_push();
            }
            if ui
                .add_enabled(git_available && gh_available, egui::Button::new("Publish"))
                .clicked()
            {
                self.run_gh_repo_create();
            }
        });
        ui.add_space(4.0);
        ui.horizontal(|ui| {
            ui.label("message");
            ui.text_edit_singleline(&mut self.git_commit_message);
        });
        ui.add_space(6.0);
        ScrollArea::vertical().id_salt("git_status_scroll").show(ui, |ui| {
            let mut status = self.git_status.clone();
            ui.add(
                TextEdit::multiline(&mut status)
                    .font(egui::TextStyle::Monospace)
                    .desired_rows(24)
                    .interactive(false),
            );
        });
    }

    fn run_git_init(&mut self) {
        let working_directory = self.working_directory();
        match run_command_in_dir("git", ["init"], &working_directory) {
            Ok(output) => {
                self.status_message = "Initialized git repository".to_owned();
                if !output.trim().is_empty() {
                    self.git_status = output;
                }
                self.refresh_git_status();
                self.refresh_external_tool_statuses();
            }
            Err(err) => {
                self.status_message = format!("git init failed: {err}");
            }
        }
    }

    fn run_git_commit_all(&mut self) {
        let working_directory = self.working_directory();
        let message = if self.git_commit_message.trim().is_empty() {
            "Update from camellia-editor".to_owned()
        } else {
            self.git_commit_message.trim().to_owned()
        };

        match run_command_in_dir("git", ["add", "-A"], &working_directory)
            .and_then(|_| run_command_in_dir("git", ["commit", "-m", &message], &working_directory))
        {
            Ok(output) => {
                self.status_message = "Committed changes".to_owned();
                if !output.trim().is_empty() {
                    self.git_status = output;
                }
                self.refresh_git_status();
            }
            Err(err) => {
                self.status_message = format!("git commit failed: {err}");
            }
        }
    }

    fn run_git_push(&mut self) {
        let working_directory = self.working_directory();
        match run_command_in_dir("git", ["push"], &working_directory) {
            Ok(output) => {
                self.status_message = "Pushed changes".to_owned();
                if !output.trim().is_empty() {
                    self.git_status = output;
                }
                self.refresh_git_status();
            }
            Err(err) => {
                self.status_message = format!("git push failed: {err}");
            }
        }
    }

    fn run_gh_repo_create(&mut self) {
        let working_directory = self.working_directory();
        let repo_name = display_name(&working_directory);
        match run_command_in_dir(
            "gh",
            ["repo", "create", &repo_name, "--source=.", "--private", "--push"],
            &working_directory,
        ) {
            Ok(output) => {
                self.status_message = "Published repository".to_owned();
                if !output.trim().is_empty() {
                    self.git_status = output;
                }
                self.refresh_git_status();
                self.refresh_external_tool_statuses();
            }
            Err(err) => {
                self.status_message = format!("gh repo create failed: {err}");
            }
        }
    }

    fn show_inspector_panel(&mut self, ctx: &egui::Context) {
        egui::SidePanel::right("inspector_panel")
            .resizable(true)
            .default_width(300.0)
            .min_width(220.0)
            .show(ctx, |ui| {
                let visible_diagnostic_count = self
                    .analysis
                    .diagnostics
                    .iter()
                    .filter(|diagnostic| diagnostic.kind != DiagnosticKind::Spelling)
                    .count();
                ui.heading("Diagnostics");
                ui.label(format!(
                    "{} issue(s), {} line(s), {} char(s)",
                    visible_diagnostic_count,
                    self.analysis.line_count,
                    self.analysis.char_count
                ));
                ui.add_space(6.0);

                ScrollArea::vertical().show(ui, |ui| {
                    if self.analysis.diagnostics.is_empty() {
                        ui.colored_label(
                            Color32::from_rgb(80, 160, 90),
                            "No structural issues found",
                        );
                    }

                    for diagnostic in self
                        .analysis
                        .diagnostics
                        .iter()
                        .filter(|diagnostic| diagnostic.kind != DiagnosticKind::Spelling)
                    {
                        let color = match diagnostic.severity {
                            Severity::Info => Color32::from_rgb(80, 140, 220),
                            Severity::Warning => Color32::from_rgb(220, 170, 60),
                            Severity::Error => Color32::from_rgb(210, 80, 80),
                        };

                        ui.group(|ui| {
                            ui.colored_label(
                                color,
                                format!(
                                    "{} at line {}",
                                    diagnostic.severity.as_str(),
                                    diagnostic.line
                                ),
                            );
                            let button = egui::Button::new(
                                RichText::new(&diagnostic.message).color(color),
                            )
                            .fill(Color32::TRANSPARENT)
                            .stroke(egui::Stroke::NONE);
                            if ui.add(button).clicked() {
                                let target = diagnostic
                                    .start_char
                                    .unwrap_or_else(|| line_start_char(&self.text, diagnostic.line));
                                self.pending_cursor_jump = Some(target);
                                self.pending_scroll_jump = Some(target);
                            }
                        });
                        ui.add_space(6.0);
                    }
                });
            });
    }

    fn show_editor(&mut self, ctx: &egui::Context) {
        egui::CentralPanel::default().show(ctx, |ui| {
            self.show_math_preview(ui);
            ui.add_space(10.0);
            ui.horizontal(|ui| {
                ui.heading("Editor");
                ui.separator();
                ui.small("Ctrl+S / Ctrl+B");
                if self
                    .current_path
                    .as_deref()
                    .map(is_read_only_editor_path)
                    .unwrap_or(false)
                {
                    ui.separator();
                    ui.label(RichText::new("read-only").italics().color(Color32::from_rgb(180, 180, 180)));
                }
            });
            ui.add_space(6.0);

            if self.showing_directory_placeholder() {
                ui.with_layout(
                    Layout::top_down(Align::Center).with_main_align(Align::Center),
                    |ui| {
                        ui.add_space(ui.available_height() * 0.25);
                        ui.heading("Directory");
                        ui.add_space(8.0);
                        let button_text = if self.opened_directory.is_some() {
                            "Change Directory"
                        } else {
                            "Open Directory"
                        };
                        if ui.button(button_text).clicked() {
                            self.open_directory();
                        }
                        ui.add_space(10.0);
                        if let Some(path) = &self.opened_directory {
                            ui.label(RichText::new(path.display().to_string()).monospace().size(18.0));
                        }
                    },
                );
                return;
            }

            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.active_center_tab, CenterPanelTab::Tex, "tex");
                ui.selectable_value(&mut self.active_center_tab, CenterPanelTab::Pdf, "pdf");
            });
            ui.separator();
            ui.add_space(6.0);

            if self.active_center_tab == CenterPanelTab::Pdf {
                ScrollArea::vertical()
                    .id_salt("pdf_preview_scroll")
                    .show(ui, |ui| {
                        if !self.pdf_preview_textures.is_empty() {
                            ui.vertical_centered(|ui| {
                                for texture in &self.pdf_preview_textures {
                                    let available_width = ui.available_width().max(1.0);
                                    let texture_size = texture.size_vec2();
                                    let scale = (available_width / texture_size.x).min(1.0);
                                    let size = texture_size * scale;
                                    ui.add(
                                        egui::Image::from_texture(texture).fit_to_exact_size(size),
                                    );
                                    ui.add_space(12.0);
                                }
                            });
                        } else if let Some(error) = &self.pdf_preview_render_error {
                            ui.label(RichText::new(error).color(Color32::from_rgb(220, 120, 120)));
                        } else {
                            ui.label("No PDF");
                        }
                    });
                return;
            }

            let diagnostics = self.analysis.diagnostics.clone();
            let mut layouter = move |ui: &egui::Ui, text: &str, wrap_width: f32| {
                let mut job = highlight_tex(text, &diagnostics);
                job.wrap.max_width = wrap_width;
                ui.fonts(|fonts| fonts.layout_job(job))
            };
            let pending_completions =
                completion_candidates(&self.text, self.last_cursor_index, &self.lsp_completion_items)
                    .map(|(_, _, completions)| completions);
            let popup_active = pending_completions
                .as_ref()
                .map(|items| !items.is_empty())
                .unwrap_or(false);
            let consumed_arrow_down = popup_active
                && ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::ArrowDown));
            let consumed_arrow_up =
                popup_active && ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::ArrowUp));
            let consumed_enter =
                popup_active && ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::Enter));
            let consumed_tab =
                popup_active && ctx.input_mut(|i| i.consume_key(Modifiers::NONE, Key::Tab));

            let gutter_width = ((self.analysis.line_count.max(1) as f32).log10().floor() + 1.0)
                .max(2.0)
                * 10.0
                + 16.0;
            let pending_scroll_target = self.pending_scroll_jump;

            let read_only = self
                .current_path
                .as_deref()
                .map(is_read_only_editor_path)
                .unwrap_or(false);
            let scroll_output = ScrollArea::both()
                .auto_shrink([false, false])
                .show(ui, |ui| {
                    ui.horizontal(|ui| {
                        let (gutter_slot_rect, _) = ui.allocate_exact_size(
                            egui::vec2(gutter_width, ui.available_height()),
                            egui::Sense::hover(),
                        );
                        let output = if read_only {
                            let mut display_text = self.text.clone();
                            TextEdit::multiline(&mut display_text)
                                .id_source("main_tex_editor")
                                .font(egui::TextStyle::Monospace)
                                .code_editor()
                                .desired_width(f32::INFINITY)
                                .desired_rows(30)
                                .lock_focus(true)
                                .layouter(&mut layouter)
                                .show(ui)
                        } else {
                            TextEdit::multiline(&mut self.text)
                                .id_source("main_tex_editor")
                                .font(egui::TextStyle::Monospace)
                                .code_editor()
                                .desired_width(f32::INFINITY)
                                .desired_rows(30)
                                .lock_focus(true)
                                .hint_text("% Start writing LaTeX here")
                                .layouter(&mut layouter)
                                .show(ui)
                        };
                        if let Some(target) = pending_scroll_target {
                            let cursor_rect = output
                                .galley
                                .pos_from_ccursor(CCursor::new(target))
                                .translate(output.galley_pos.to_vec2())
                                .expand2(egui::vec2(24.0, 36.0));
                            ui.scroll_to_rect(cursor_rect, Some(Align::Center));
                        }
                        let gutter_rect = egui::Rect::from_min_size(
                            egui::pos2(gutter_slot_rect.min.x, output.response.rect.min.y),
                            egui::vec2(gutter_width, output.response.rect.height()),
                        );
                        paint_line_number_gutter(ui, gutter_rect, &output, self.analysis.line_count);
                        output
                    })
                });
            let output = scroll_output.inner.inner;

            let popup_data = output.cursor_range.as_ref().and_then(|cursor_range| {
                completion_candidates(&self.text, self.last_cursor_index, &self.lsp_completion_items)
                    .map(|(start_char, end_char, completions)| {
                        let cursor_rect = output
                            .galley
                            .pos_from_cursor(&cursor_range.primary)
                            .translate(output.galley_pos.to_vec2());
                        (cursor_rect.left_bottom(), start_char, end_char, completions)
                    })
            });

            if !read_only && output.response.changed() {
                self.recompute_current_dirty();
                self.status_message = "Editing TeX".to_owned();
                self.refresh_analysis();
                self.sync_texlab_document();
                self.math_preview_edit_deadline = Some(Instant::now() + MATH_PREVIEW_IDLE_DELAY);
            }

            if let Some(target) = self.pending_cursor_jump.take() {
                let mut state = output.state.clone();
                state
                    .cursor
                    .set_char_range(Some(CCursorRange::one(CCursor::new(target))));
                state.store(ctx, output.response.id);
                ctx.memory_mut(|mem| mem.request_focus(output.response.id));
                self.last_cursor_index = Some(target);
                self.active_center_tab = CenterPanelTab::Tex;
                ctx.request_repaint();
            }

            if self.pending_scroll_jump.is_some() {
                self.pending_scroll_jump = None;
                ctx.request_repaint();
            }

            if let Some(state) = TextEdit::load_state(ctx, output.response.id) {
                if let Some(range) = state.cursor.char_range() {
                    let cursor_index = range.primary.index;
                    if self.last_cursor_index != Some(cursor_index) {
                        self.active_math_preview = math_preview_at_cursor(&self.text, cursor_index);
                        self.math_preview_edit_deadline =
                            Some(Instant::now() + MATH_PREVIEW_IDLE_DELAY);
                        self.last_cursor_index = Some(cursor_index);
                        self.request_texlab_completion(cursor_index);
                    }
                } else {
                    self.active_math_preview = None;
                    self.last_cursor_index = None;
                }
            }

            if let Some((popup_pos, start_char, end_char, completions)) = popup_data {
                let completions: Vec<LspCompletionItem> = completions.to_vec();
                let environment_context = completion_is_environment_context(&self.text, start_char);
                if completions.is_empty() {
                    self.completion_selected_index = 0;
                    return;
                }
                self.completion_selected_index =
                    self.completion_selected_index.min(completions.len().saturating_sub(1));

                let mut apply_index = None;
                if output.response.has_focus() {
                    if consumed_arrow_down {
                        self.completion_selected_index =
                            (self.completion_selected_index + 1) % completions.len();
                    }
                    if consumed_arrow_up {
                        self.completion_selected_index = if self.completion_selected_index == 0 {
                            completions.len() - 1
                        } else {
                            self.completion_selected_index - 1
                        };
                    }
                    if consumed_tab || consumed_enter {
                        apply_index = Some(self.completion_selected_index);
                    }
                }

                if let Some(index) = apply_index {
                    if let Some(item) = completions.get(index) {
                        self.apply_completion(start_char, end_char, &item.insert_text);
                    }
                }

                egui::Area::new(egui::Id::new("completion_popup"))
                    .order(egui::Order::Foreground)
                    .fixed_pos(popup_pos + egui::vec2(0.0, 6.0))
                    .show(ctx, |ui| {
                        egui::Frame::popup(ui.style()).show(ui, |ui| {
                            ui.set_min_width(260.0);
                            ui.set_max_width(320.0);
                            ScrollArea::vertical()
                                .id_salt("completion_popup_scroll")
                                .max_height(300.0)
                                .show(ui, |ui| {
                                    for (index, item) in completions.iter().enumerate() {
                                        let selected = index == self.completion_selected_index;
                                        let (kind_label, kind_color) =
                                            completion_kind_badge(item, environment_context);
                                        let mut clicked = false;
                                        egui::Frame::NONE
                                            .fill(if selected {
                                                Color32::from_rgb(40, 52, 74)
                                            } else {
                                                Color32::TRANSPARENT
                                            })
                                            .corner_radius(6.0)
                                            .inner_margin(egui::Margin::symmetric(8, 3))
                                            .show(ui, |ui| {
                                                let response = ui
                                                    .horizontal(|ui| {
                                                        ui.label(
                                                            RichText::new(format!(" {} ", kind_label))
                                                                .monospace()
                                                                .small()
                                                                .color(Color32::WHITE)
                                                                .background_color(kind_color),
                                                        );
                                                        let label = if item.deprecated {
                                                            RichText::new(&item.label)
                                                                .strikethrough()
                                                                .color(Color32::from_rgb(170, 170, 170))
                                                        } else {
                                                            RichText::new(&item.label)
                                                                .strong()
                                                                .color(completion_label_color(
                                                                    item,
                                                                    environment_context,
                                                                ))
                                                        };
                                                        ui.label(label);
                                                        ui.with_layout(
                                                            Layout::right_to_left(Align::Center),
                                                            |ui| {
                                                                if let Some(detail) =
                                                                    completion_detail_preview(item)
                                                                {
                                                                    ui.label(
                                                                        RichText::new(detail)
                                                                            .small()
                                                                            .monospace()
                                                                            .color(Color32::from_rgb(
                                                                                138, 160, 183,
                                                                            )),
                                                                    );
                                                                }
                                                            },
                                                        );
                                                    })
                                                    .response;
                                                if selected {
                                                    ui.scroll_to_rect(response.rect, Some(Align::Center));
                                                }
                                                clicked = response.clicked();
                                            });
                                        if clicked {
                                            self.completion_selected_index = index;
                                            self.apply_completion(
                                                start_char,
                                                end_char,
                                                &item.insert_text,
                                            );
                                        }
                                    }
                                });
                        });
                    });
            } else {
                self.completion_selected_index = 0;
            }
        });
    }

    fn show_status_bar(&mut self, ctx: &egui::Context) {
        egui::TopBottomPanel::bottom("status_bar")
            .show_separator_line(false)
            .exact_height(26.0)
            .show(ctx, |ui| {
                ui.horizontal_wrapped(|ui| {
                    ui.label(&self.status_message);
                    if self.build_running {
                        ui.separator();
                        ui.spinner();
                    }
                    ui.separator();
                    let (save_text, save_color) = if self.has_unsaved_workspace_changes() {
                        ("unsaved", Color32::from_rgb(220, 170, 60))
                    } else {
                        ("saved", Color32::from_rgb(90, 190, 120))
                    };
                    ui.label(RichText::new(save_text).color(save_color).strong());
                    ui.separator();
                    ui.label(format!("{} lines", self.analysis.line_count));
                    ui.separator();
                    ui.label(format!("{} chars", self.analysis.char_count));
                    ui.separator();
                    ui.label(format!("{} labels", self.analysis.label_count));
                    ui.separator();
                    ui.label(format!("{} refs", self.analysis.reference_count));
                    if self.active_math_preview.is_some() {
                        ui.separator();
                        ui.label("math under cursor");
                    }
                    if !self.external_tool_statuses.is_empty() {
                        ui.separator();
                        ui.label("tools:");
                        for tool in &self.external_tool_statuses {
                            let (text, color) = if tool.path.is_some() {
                                (format!("{} ok", tool.name), Color32::from_rgb(90, 190, 120))
                            } else {
                                (format!("{} missing", tool.name), Color32::from_rgb(210, 90, 90))
                            };
                            let response = ui.label(RichText::new(text).color(color).monospace());
                            let hover = match (&tool.path, &tool.detail) {
                                (Some(path), Some(detail)) => format!("{path}\n{detail}"),
                                (Some(path), None) => path.clone(),
                                (None, Some(detail)) => detail.clone(),
                                (None, None) => "not found".to_owned(),
                            };
                            response.on_hover_text(hover);
                        }
                    }
                });
            });
    }
}

impl Default for TexEditorApp {
    fn default() -> Self {
        let settings = load_app_settings();
        let text = String::new();
        let analysis = analyze_tex(&text, None, &settings);
        let active_math_preview = math_preview_at_cursor(&text, 0);
        let (texlab_sender, texlab_receiver, texlab_status) = start_texlab_client()
            .map(|(sender, receiver)| (Some(sender), Some(receiver), Some("starting texlab".to_owned())))
            .unwrap_or((None, None, None));
        let mut app = Self {
            settings,
            show_settings_window: false,
            show_templates_window: false,
            text,
            current_path: None,
            opened_directory: None,
            recent_directories: load_recent_directories(),
            file_tree: build_file_node(Path::new(".")),
            file_tree_dirty: false,
            active_left_tab: LeftPanelTab::Tex,
            active_center_tab: CenterPanelTab::Tex,
            git_status: "Open a directory to inspect git status.\n".to_owned(),
            dirty: false,
            file_buffers: HashMap::new(),
            status_message: "TeX editor ready".to_owned(),
            build_log: "Build output will appear here.\n".to_owned(),
            build_tool: None,
            build_receiver: None,
            build_running: false,
            last_build_result: None,
            analysis,
            active_math_preview,
            math_preview_texture: None,
            math_preview_render_error: None,
            math_preview_render_key: None,
            math_preview_requested_key: None,
            math_preview_receiver: None,
            math_preview_running: false,
            math_preview_edit_deadline: None,
            last_cursor_index: None,
            pdf_preview_textures: Vec::new(),
            pdf_preview_render_error: None,
            pdf_preview_render_key: None,
            selected_pdf_path: None,
            template_entries: Vec::new(),
            selected_template_tex_path: None,
            template_copy_file_name: String::new(),
            template_preview_textures: Vec::new(),
            template_preview_render_error: None,
            template_preview_render_key: None,
            template_delete_confirm_path: None,
            texlab_sender,
            texlab_receiver,
            texlab_status,
            completion_request_serial: 0,
            latest_completion_serial: 0,
            lsp_completion_items: Vec::new(),
            completion_selected_index: 0,
            pending_cursor_jump: None,
            pending_scroll_jump: None,
            selected_tree_path: None,
            file_clipboard: None,
            file_new_name: String::new(),
            git_commit_message: "Update from camellia-editor".to_owned(),
            external_tool_statuses: Vec::new(),
            confirm_close_requested: false,
            allow_immediate_close: false,
        };
        app.refresh_external_tool_statuses();
        if let Some(target) = launch_target_path() {
            if target.is_dir() {
                app.set_opened_directory(target);
            } else if target.is_file() {
                if let Some(parent) = target.parent() {
                    app.register_recent_directory(parent);
                    app.opened_directory = Some(parent.to_path_buf());
                    app.file_tree = build_file_node(parent);
                    app.file_tree_dirty = false;
                }
                if target
                    .extension()
                    .and_then(|ext| ext.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("pdf"))
                    .unwrap_or(false)
                {
                    app.open_pdf_at_path(target);
                } else {
                    app.open_document_at_path(target);
                }
            }
        }
        app
    }
}

impl App for TexEditorApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame) {
        self.handle_close_request(ctx);
        self.handle_shortcuts(ctx);
        self.poll_build_status();
        self.poll_math_preview_status(ctx);
        self.poll_texlab_events();
        ctx.send_viewport_cmd(ViewportCommand::Title(self.window_title()));

        self.sync_math_preview_render(ctx);
        self.sync_pdf_preview_render(ctx);
        self.show_toolbar(ctx);
        self.show_settings_window(ctx);
        self.show_templates_window(ctx);
        self.show_template_delete_confirm_dialog(ctx);
        self.show_workspace_panel(ctx);
        self.show_inspector_panel(ctx);
        self.show_status_bar(ctx);
        self.show_editor(ctx);
        self.show_close_confirm_dialog(ctx);
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum LeftPanelTab {
    Tex,
    File,
    Git,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum CenterPanelTab {
    Tex,
    Pdf,
}

#[derive(Clone)]
struct MathPreview {
    mode: String,
    source: String,
    line: usize,
}

struct BuildOutcome {
    success: bool,
    status_message: String,
    log: String,
    pdf_path: Option<PathBuf>,
}

struct MathPreviewOutcome {
    render_key: String,
    image: Result<egui::ColorImage, String>,
}

struct ResolvedBuildTool {
    kind: BuildToolPreference,
    path: String,
}

fn render_math_preview_image(
    preview: &MathPreview,
    preamble: &str,
) -> Result<egui::ColorImage, String> {
    let tex_engine = resolve_command_path("lualatex")
        .ok_or_else(|| "`lualatex` was not found on PATH or known install paths.".to_owned())?;
    let pdftoppm = resolve_command_path("pdftoppm")
        .ok_or_else(|| "`pdftoppm` was not found on PATH or known install paths.".to_owned())?;

    let render_dir = std::env::temp_dir().join("tex-editor-math-preview");
    fs::create_dir_all(&render_dir)
        .map_err(|err| format!("Could not create preview temp directory: {err}"))?;

    let tex_path = render_dir.join("preview.tex");
    let pdf_path = render_dir.join("preview.pdf");
    let ppm_path = render_dir.join("preview.ppm");
    let texmf_dir = std::env::temp_dir().join("tex-editor-texmf-preview");
    let texmf_var = texmf_dir.join("var");
    let texmf_config = texmf_dir.join("config");

    fs::write(&tex_path, build_preview_document(preview, preamble))
        .map_err(|err| format!("Could not write preview source: {err}"))?;
    fs::create_dir_all(&texmf_var)
        .map_err(|err| format!("Could not create preview TEXMFVAR directory: {err}"))?;
    fs::create_dir_all(&texmf_config)
        .map_err(|err| format!("Could not create preview TEXMFCONFIG directory: {err}"))?;

    let latex_output = configure_command(Command::new(&tex_engine))
        .args([
            "-interaction=nonstopmode",
            "-halt-on-error",
            "-file-line-error",
            "preview.tex",
        ])
        .current_dir(&render_dir)
        .env("TEXMFVAR", &texmf_var)
        .env("TEXMFCONFIG", &texmf_config)
        .output()
        .map_err(|err| format!("Could not launch `{tex_engine}`: {err}"))?;

    if !latex_output.status.success() {
        return Err(format!(
            "TeX render failed.\n{}",
            summarize_command_output(&latex_output.stdout, &latex_output.stderr)
        ));
    }

    let image_output = configure_command(Command::new(&pdftoppm))
        .args(["-singlefile", "-r", "144", "preview.pdf", "preview"])
        .current_dir(&render_dir)
        .output()
        .map_err(|err| format!("Could not launch `{pdftoppm}`: {err}"))?;

    if !image_output.status.success() {
        return Err(format!(
            "Preview image conversion failed.\n{}",
            summarize_command_output(&image_output.stdout, &image_output.stderr)
        ));
    }

    if !pdf_path.exists() {
        return Err("TeX render did not produce preview.pdf.".to_owned());
    }
    if !ppm_path.exists() {
        return Err("Image conversion did not produce preview.ppm.".to_owned());
    }

    let bytes = fs::read(&ppm_path).map_err(|err| format!("Could not read preview image: {err}"))?;
    parse_ppm_image(&bytes).map_err(|err| format!("Could not parse preview image: {err}"))
}

fn run_build_command(
    tool: String,
    args: Vec<String>,
    project_root: PathBuf,
    path: PathBuf,
) -> BuildOutcome {
    let texmf_dir = std::env::temp_dir().join("tex-editor-texmf");
    let texmf_var = texmf_dir.join("var");
    let texmf_config = texmf_dir.join("config");
    let _ = fs::create_dir_all(&texmf_var);
    let _ = fs::create_dir_all(&texmf_config);

    let output = configure_command(Command::new(&tool))
        .args(&args)
        .current_dir(&project_root)
        .env("TEXMFVAR", &texmf_var)
        .env("TEXMFCONFIG", &texmf_config)
        .output();

    match output {
        Ok(output) => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            let log = format!(
                "$ {} {}\n\n{}{}",
                tool,
                args.join(" "),
                stdout,
                stderr
            );

            if output.status.success() {
                BuildOutcome {
                    success: true,
                    status_message: format!("Build succeeded: {}", output_pdf_path(&path).display()),
                    log,
                    pdf_path: Some(output_pdf_path(&path)),
                }
            } else {
                BuildOutcome {
                    success: false,
                    status_message: format!("Build failed with {tool}"),
                    log,
                    pdf_path: None,
                }
            }
        }
        Err(err) => BuildOutcome {
            success: false,
            status_message: format!("Build failed: {err}"),
            log: format!("Could not launch {tool}: {err}\n"),
            pdf_path: None,
        },
    }
}

fn resolve_preferred_build_tool(preference: BuildToolPreference) -> Option<ResolvedBuildTool> {
    let latexmk = resolve_command_path("latexmk");
    let tectonic = resolve_command_path("tectonic");
    match preference {
        BuildToolPreference::Auto => latexmk
            .map(|path| ResolvedBuildTool {
                kind: BuildToolPreference::Latexmk,
                path,
            })
            .or_else(|| {
                tectonic.map(|path| ResolvedBuildTool {
                    kind: BuildToolPreference::Tectonic,
                    path,
                })
            }),
        BuildToolPreference::Latexmk => latexmk
            .map(|path| ResolvedBuildTool {
                kind: BuildToolPreference::Latexmk,
                path,
            })
            .or_else(|| {
                tectonic.map(|path| ResolvedBuildTool {
                    kind: BuildToolPreference::Tectonic,
                    path,
                })
            }),
        BuildToolPreference::Tectonic => tectonic
            .map(|path| ResolvedBuildTool {
                kind: BuildToolPreference::Tectonic,
                path,
            })
            .or_else(|| {
                latexmk.map(|path| ResolvedBuildTool {
                    kind: BuildToolPreference::Latexmk,
                    path,
                })
            }),
    }
}

fn describe_detected_build_tools() -> String {
    let mut tools = Vec::new();
    if resolve_command_path("latexmk").is_some() {
        tools.push("latexmk");
    }
    if resolve_command_path("tectonic").is_some() {
        tools.push("tectonic");
    }
    if tools.is_empty() {
        "none".to_owned()
    } else {
        tools.join(", ")
    }
}

fn build_preview_document(preview: &MathPreview, preamble: &str) -> String {
    format!(
        "\\documentclass{{article}}\n\\usepackage[active,tightpage]{{preview}}\n\\PreviewEnvironment{{preview}}\n\\setlength{{\\PreviewBorder}}{{14pt}}\n{preamble}\n\\pagestyle{{empty}}\n\\begin{{document}}\n\\begin{{preview}}\n{}\n\\end{{preview}}\n\\end{{document}}\n",
        wrap_preview_source(preview)
    )
}

fn should_surface_math_preview_error(err: &str) -> bool {
    err.contains("was not found on PATH")
        || err.contains("Could not create")
        || err.contains("Could not write")
        || err.contains("Could not launch")
        || err.contains("Image conversion did not produce")
        || err.contains("Could not read preview image")
        || err.contains("Could not parse preview image")
}

fn file_name_for_build(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("document.tex")
        .to_owned()
}

fn output_pdf_path(path: &Path) -> PathBuf {
    path.parent()
        .unwrap_or_else(|| Path::new("."))
        .join("out")
        .join(
            path.file_name()
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("document.tex")),
        )
        .with_extension("pdf")
}

fn current_pdf_preview_path(current_path: Option<&Path>, selected_pdf_path: Option<&Path>) -> Option<PathBuf> {
    if let Some(pdf_path) = selected_pdf_path {
        return pdf_path.exists().then(|| pdf_path.to_path_buf());
    }
    let path = current_path?;
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    if extension != "tex" {
        return None;
    }

    let pdf_path = output_pdf_path(path);
    pdf_path.exists().then_some(pdf_path)
}

fn corresponding_tex_path(pdf_path: &Path) -> Option<PathBuf> {
    let candidate = if pdf_path
        .parent()
        .and_then(Path::file_name)
        .and_then(|name| name.to_str())
        .map(|name| name.eq_ignore_ascii_case("out"))
        .unwrap_or(false)
    {
        pdf_path
            .parent()
            .and_then(Path::parent)
            .unwrap_or_else(|| Path::new("."))
            .join(
                pdf_path
                    .file_name()
                    .map(PathBuf::from)
                    .unwrap_or_else(|| PathBuf::from("document.pdf")),
            )
            .with_extension("tex")
    } else {
        pdf_path.with_extension("tex")
    };
    candidate.exists().then_some(candidate)
}

fn pdf_render_cache_key(pdf_path: &Path) -> String {
    let metadata = fs::metadata(pdf_path).ok();
    let modified = metadata
        .as_ref()
        .and_then(|meta| meta.modified().ok())
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|duration| duration.as_secs())
        .unwrap_or_default();
    let length = metadata.map(|meta| meta.len()).unwrap_or_default();
    format!("{}:{modified}:{length}", pdf_path.display())
}

fn render_pdf_preview_images(pdf_path: &Path) -> Result<Vec<egui::ColorImage>, String> {
    let pdftoppm = resolve_command_path("pdftoppm")
        .ok_or_else(|| "`pdftoppm` was not found on PATH or known install paths.".to_owned())?;

    let render_dir = std::env::temp_dir().join("tex-editor-pdf-preview");
    let _ = fs::remove_dir_all(&render_dir);
    fs::create_dir_all(&render_dir)
        .map_err(|err| format!("Could not create PDF preview directory: {err}"))?;

    let output = configure_command(Command::new(&pdftoppm))
        .args(["-r", "144"])
        .arg(pdf_path)
        .arg(render_dir.join("page"))
        .output()
        .map_err(|err| format!("Could not launch `{pdftoppm}`: {err}"))?;

    if !output.status.success() {
        return Err(format!(
            "PDF preview conversion failed.\n{}",
            summarize_command_output(&output.stdout, &output.stderr)
        ));
    }

    let mut ppm_paths: Vec<_> = fs::read_dir(&render_dir)
        .map_err(|err| format!("Could not read PDF preview directory: {err}"))?
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .and_then(|ext| ext.to_str())
                .map(|ext| ext.eq_ignore_ascii_case("ppm"))
                .unwrap_or(false)
        })
        .collect();
    ppm_paths.sort();

    if ppm_paths.is_empty() {
        return Err("No previewable PDF pages were produced.".to_owned());
    }

    ppm_paths
        .into_iter()
        .map(|path| {
            let bytes = fs::read(&path)
                .map_err(|err| format!("Could not read PDF preview page `{}`: {err}", path.display()))?;
            parse_ppm_image(&bytes)
                .map_err(|err| format!("Could not parse PDF preview page `{}`: {err}", path.display()))
        })
        .collect()
}

fn discover_template_entries() -> Vec<TemplateEntry> {
    let mut entries = Vec::new();
    let mut seen = HashSet::new();
    for templates_dir in template_root_candidates() {
        collect_template_entries(&templates_dir, &templates_dir, &mut seen, &mut entries);
    }
    entries.sort_by(|a, b| a.label.cmp(&b.label));
    entries
}

fn template_root_candidates() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    let mut seen = HashSet::new();
    if let Some(candidate) = writable_templates_path() {
        if candidate.is_dir() && seen.insert(candidate.clone()) {
            roots.push(candidate);
        }
    }
    for root in app_search_roots() {
        let candidate = root.join("templates");
        if candidate.is_dir() && seen.insert(candidate.clone()) {
            roots.push(candidate);
        }
    }
    roots
}

fn primary_template_root() -> PathBuf {
    writable_templates_path()
        .or_else(|| app_search_roots().into_iter().next().map(|root| root.join("templates")))
        .unwrap_or_else(|| PathBuf::from("templates"))
}

fn bundled_template_roots() -> Vec<PathBuf> {
    let writable_root = writable_templates_path();
    let mut roots = Vec::new();
    let mut seen = HashSet::new();
    for root in app_search_roots() {
        let candidate = root.join("templates");
        if writable_root.as_ref() == Some(&candidate) {
            continue;
        }
        if candidate.is_dir() && seen.insert(candidate.clone()) {
            roots.push(candidate);
        }
    }
    roots
}

fn ensure_default_templates_installed() {
    let Some(writable_root) = writable_templates_path() else {
        return;
    };
    if directory_has_files(&writable_root) {
        return;
    }

    let Some(default_root) = bundled_template_roots().into_iter().next() else {
        return;
    };

    let _ = copy_path_recursively(&default_root, &writable_root);
}

fn directory_has_files(path: &Path) -> bool {
    fs::read_dir(path)
        .ok()
        .and_then(|mut entries| entries.next())
        .is_some()
}

fn template_default_copy_name(tex_path: &Path) -> String {
    tex_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("template.tex")
        .to_owned()
}

fn normalize_template_copy_name(name: &str) -> Option<String> {
    let trimmed = name.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut sanitized: String = trimmed
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            _ => ch,
        })
        .collect();
    if sanitized.is_empty() {
        return None;
    }
    if !sanitized.to_ascii_lowercase().ends_with(".tex") {
        sanitized.push_str(".tex");
    }
    Some(sanitized)
}

fn collect_template_entries(
    templates_root: &Path,
    dir: &Path,
    seen: &mut HashSet<PathBuf>,
    entries: &mut Vec<TemplateEntry>,
) {
    let Ok(children) = fs::read_dir(dir) else {
        return;
    };

    for child in children.flatten() {
        let path = child.path();
        if path.is_dir() {
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .map(|name| name.eq_ignore_ascii_case("out"))
                .unwrap_or(false)
            {
                continue;
            }
            collect_template_entries(templates_root, &path, seen, entries);
            continue;
        }

        let is_tex = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("tex"))
            .unwrap_or(false);
        if !is_tex {
            continue;
        }

        let pdf_path = output_pdf_path(&path);
        if !pdf_path.exists() || !seen.insert(path.clone()) {
            continue;
        }

        let relative = path
            .strip_prefix(templates_root)
            .unwrap_or(&path)
            .display()
            .to_string();
        entries.push(TemplateEntry {
            tex_path: path,
            pdf_path,
            label: relative,
        });
    }
}

fn read_git_status(working_directory: &Path) -> String {
    let Some(git) = resolve_command_path("git") else {
        return "`git` was not found on PATH or known install paths.\n".to_owned();
    };

    let output = configure_command(Command::new(git))
        .args(["status", "--short", "--branch"])
        .current_dir(working_directory)
        .output();

    match output {
        Ok(output) if output.status.success() => {
            let stdout = String::from_utf8_lossy(&output.stdout);
            if stdout.trim().is_empty() {
                "Working tree clean.\n".to_owned()
            } else {
                stdout.into_owned()
            }
        }
        Ok(output) => {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.trim().is_empty() {
                "Not a git repository.\n".to_owned()
            } else {
                stderr.into_owned()
            }
        }
        Err(err) => format!("Could not query git status: {err}\n"),
    }
}

fn run_command_in_dir<const N: usize>(
    command: &str,
    args: [&str; N],
    working_directory: &Path,
) -> Result<String, String> {
    let resolved =
        resolve_command_path(command).ok_or_else(|| format!("`{command}` was not found"))?;
    let output = configure_command(Command::new(resolved))
        .args(args)
        .current_dir(working_directory)
        .output()
        .map_err(|err| format!("Could not launch `{command}`: {err}"))?;

    if output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        if stdout.is_empty() {
            Ok(stderr)
        } else if stderr.is_empty() {
            Ok(stdout)
        } else {
            Ok(format!("{stdout}\n{stderr}"))
        }
    } else {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_owned();
        let stdout = String::from_utf8_lossy(&output.stdout).trim().to_owned();
        let message = if !stderr.is_empty() { stderr } else { stdout };
        Err(if message.is_empty() {
            format!("`{command}` exited with {}", output.status)
        } else {
            message
        })
    }
}

fn latexmk_build_args(path: &Path, project_root: &Path) -> Result<Vec<String>, String> {
    let rc_path = write_bundled_latexmkrc()?;
    let out_dir = project_root.join("out");
    fs::create_dir_all(&out_dir).map_err(|err| format!("Could not create out directory: {err}"))?;
    let source_arg = path
        .strip_prefix(project_root)
        .unwrap_or(path)
        .to_string_lossy()
        .into_owned();
    let args = vec![
        "-r".to_owned(),
        rc_path,
        format!("-outdir={}", out_dir.to_string_lossy()),
        format!("-auxdir={}", out_dir.to_string_lossy()),
        "-interaction=nonstopmode".to_owned(),
        "-halt-on-error".to_owned(),
        "-file-line-error".to_owned(),
        "-synctex=1".to_owned(),
        source_arg,
    ];
    Ok(args)
}

fn write_bundled_latexmkrc() -> Result<String, String> {
    let rc_dir = std::env::temp_dir().join("tex-editor-latexmk");
    fs::create_dir_all(&rc_dir)
        .map_err(|err| format!("Could not create latexmkrc directory: {err}"))?;
    let rc_path = rc_dir.join("latexmkrc");
    fs::write(&rc_path, BUNDLED_LATEXMKRC)
        .map_err(|err| format!("Could not write bundled latexmkrc: {err}"))?;
    Ok(rc_path.to_string_lossy().into_owned())
}

fn wrap_preview_source(preview: &MathPreview) -> String {
    match preview.mode.as_str() {
        "inline $...$" | "inline \\(...\\)" => format!("${}$", preview.source),
        "display $$...$$" => format!("$$\n{}\n$$", preview.source),
        "display \\[...\\]" => format!("\\[\n{}\n\\]", preview.source),
        mode if mode.starts_with("environment ") => {
            let env = mode.trim_start_matches("environment ").trim();
            format!(
                "\\begin{{{env}}}\n{}\n\\end{{{env}}}",
                preview.source
            )
        }
        _ => preview.source.clone(),
    }
}

fn extract_preview_preamble(text: &str) -> String {
    let before_document = text.split("\\begin{document}").next().unwrap_or_default();
    before_document
        .lines()
        .filter(|line| !line.trim_start().starts_with("\\documentclass"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn summarize_command_output(stdout: &[u8], stderr: &[u8]) -> String {
    let combined = format!(
        "{}{}",
        String::from_utf8_lossy(stdout),
        String::from_utf8_lossy(stderr)
    );
    let lines: Vec<_> = combined
        .lines()
        .rev()
        .take(12)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    if lines.is_empty() {
        "No diagnostic output was produced.".to_owned()
    } else {
        lines.join("\n")
    }
}

fn parse_ppm_image(bytes: &[u8]) -> io::Result<egui::ColorImage> {
    let mut cursor = 0usize;
    let magic = read_ppm_token(bytes, &mut cursor)?;
    if magic != "P6" {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported PPM format",
        ));
    }

    let width: usize = read_ppm_token(bytes, &mut cursor)?
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid width"))?;
    let height: usize = read_ppm_token(bytes, &mut cursor)?
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid height"))?;
    let max_value: usize = read_ppm_token(bytes, &mut cursor)?
        .parse()
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid max value"))?;
    if max_value != 255 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "unsupported color depth",
        ));
    }

    while cursor < bytes.len() && bytes[cursor].is_ascii_whitespace() {
        cursor += 1;
    }

    let expected_len = width
        .checked_mul(height)
        .and_then(|pixels| pixels.checked_mul(3))
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "image is too large"))?;

    if bytes.len().saturating_sub(cursor) < expected_len {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "pixel data is truncated",
        ));
    }

    Ok(egui::ColorImage::from_rgb(
        [width, height],
        &bytes[cursor..cursor + expected_len],
    ))
}

fn read_ppm_token<'a>(bytes: &'a [u8], cursor: &mut usize) -> io::Result<&'a str> {
    loop {
        while *cursor < bytes.len() && bytes[*cursor].is_ascii_whitespace() {
            *cursor += 1;
        }
        if *cursor < bytes.len() && bytes[*cursor] == b'#' {
            while *cursor < bytes.len() && bytes[*cursor] != b'\n' {
                *cursor += 1;
            }
            continue;
        }
        break;
    }

    if *cursor >= bytes.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "unexpected end of PPM header",
        ));
    }

    let start = *cursor;
    while *cursor < bytes.len() && !bytes[*cursor].is_ascii_whitespace() && bytes[*cursor] != b'#' {
        *cursor += 1;
    }

    std::str::from_utf8(&bytes[start..*cursor])
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid header token"))
}

#[derive(Clone)]
struct MathSpan {
    start_char: usize,
    end_char: usize,
    start_byte: usize,
    end_byte: usize,
    source: String,
    mode: String,
    line: usize,
}

#[derive(Clone, Default)]
struct TexAnalysis {
    outline: Vec<OutlineItem>,
    symbols: Vec<SymbolItem>,
    diagnostics: Vec<Diagnostic>,
    line_count: usize,
    char_count: usize,
    label_count: usize,
    reference_count: usize,
}

#[derive(Clone)]
struct OutlineItem {
    command: String,
    title: String,
    level: usize,
    line: usize,
}

#[derive(Clone)]
struct SymbolItem {
    kind: String,
    name: String,
    line: usize,
}

#[derive(Clone)]
struct Diagnostic {
    kind: DiagnosticKind,
    severity: Severity,
    line: usize,
    message: String,
    start_char: Option<usize>,
    end_char: Option<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum DiagnosticKind {
    General,
    Spelling,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Severity {
    Info,
    Warning,
    Error,
}

impl Severity {
    fn as_str(self) -> &'static str {
        match self {
            Severity::Info => "info",
            Severity::Warning => "warning",
            Severity::Error => "error",
        }
    }
}

fn consume_shortcut(ctx: &egui::Context, key: Key) -> bool {
    ctx.input_mut(|input| input.consume_shortcut(&KeyboardShortcut::new(Modifiers::CTRL, key)))
}

fn display_name(path: &Path) -> String {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| path.display().to_string())
}

const LATEX_ENVIRONMENT_COMPLETIONS: &[&str] = &[
    "align",
    "align*",
    "equation",
    "equation*",
    "gather",
    "gather*",
    "multline",
    "multline*",
    "itemize",
    "enumerate",
    "description",
    "figure",
    "table",
    "tabular",
    "center",
    "quote",
    "quotation",
    "thebibliography",
    "abstract",
    "theorem",
    "proof",
];

fn completion_candidates(
    text: &str,
    cursor_char: Option<usize>,
    lsp_items: &[LspCompletionItem],
) -> Option<(usize, usize, Vec<LspCompletionItem>)> {
    let cursor_char = cursor_char?;
    let chars: Vec<char> = text.chars().collect();
    if cursor_char > chars.len() {
        return None;
    }

    let mut start = cursor_char;
    let mut saw_backslash = false;
    while start > 0 {
        let ch = chars[start - 1];
        if ch == '\\' {
            start -= 1;
            saw_backslash = true;
            break;
        }
        if ch.is_ascii_alphabetic() {
            start -= 1;
            continue;
        }
        break;
    }

    let prefix: String = chars[start..cursor_char].iter().collect();
    if prefix.len() < 2 && !saw_backslash {
        return None;
    }

    let matches: Vec<_> = if !lsp_items.is_empty() {
        dedup_completion_items(lsp_items.to_vec())
    } else {
        Vec::new()
    };
    if matches.is_empty() {
        None
    } else {
        Some((start, cursor_char, matches))
    }
}

fn dedup_completion_items(items: Vec<LspCompletionItem>) -> Vec<LspCompletionItem> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();
    for item in items {
        if seen.insert((item.label.clone(), item.insert_text.clone())) {
            deduped.push(item);
        }
    }
    deduped
}

fn replace_char_range(text: &mut String, start_char: usize, end_char: usize, replacement: &str) {
    let start_byte = nth_char_to_byte(text, start_char);
    let end_byte = nth_char_to_byte(text, end_char);
    text.replace_range(start_byte..end_byte, replacement);
}

fn completion_replacement(
    text: &str,
    start_char: usize,
    end_char: usize,
    item: &str,
) -> (usize, String, usize, usize) {
    let normalized_item = normalize_completion_item(item);
    if completion_is_environment_context(text, start_char) && is_environment_name(&normalized_item) {
        let chars: Vec<char> = text.chars().collect();
        let replace_start_char = start_char.saturating_sub("\\begin{".chars().count());
        let mut replace_end_char = start_char.max(end_char);
        while matches!(chars.get(replace_end_char), Some(ch) if ch.is_ascii_alphabetic() || *ch == '*')
        {
            replace_end_char += 1;
        }
        while chars.get(replace_end_char) == Some(&'}') {
            replace_end_char += 1;
        }
        let replacement = format!("\\begin{{{normalized_item}}}\n    \n\\end{{{normalized_item}}}");
        let cursor_char = replace_start_char
            + format!("\\begin{{{normalized_item}}}\n    ").chars().count();
        (replace_start_char, replacement, replace_end_char, cursor_char)
    } else {
        let raw_item = item.trim().to_owned();
        let replacement = if text
            .chars()
            .nth(start_char)
            .map(|ch| ch == '\\')
            .unwrap_or(false)
            && !raw_item.starts_with('\\')
        {
            format!("\\{raw_item}")
        } else {
            raw_item
        };
        let replacement = enrich_completion_insert_text(&replacement);
        let cursor_char = cursor_position_for_completion(start_char, &replacement);
        (start_char, replacement, end_char, cursor_char)
    }
}

fn normalize_completion_item(item: &str) -> String {
    let mut value = item.trim().to_owned();
    if let Some(prefix) = value.strip_suffix("{}") {
        value = prefix.to_owned();
    }
    if let Some(prefix) = value.strip_suffix("{…}") {
        value = prefix.to_owned();
    }
    value
}

fn enrich_completion_insert_text(item: &str) -> String {
    if item.contains('{') {
        return item.to_owned();
    }

    match item {
        "\\frac" => "\\frac{}{}".to_owned(),
        "\\begin" | "\\end" | "\\section" | "\\subsection" | "\\subsubsection" | "\\emph"
        | "\\textbf" | "\\textit" | "\\includegraphics" | "\\caption" | "\\label"
        | "\\ref" | "\\eqref" | "\\cite" | "\\sqrt" | "\\mathbb" | "\\mathbf"
        | "\\mathrm" | "\\usepackage" | "\\documentclass" => format!("{item}{{}}"),
        _ => item.to_owned(),
    }
}

fn cursor_position_for_completion(start_char: usize, replacement: &str) -> usize {
    if let Some(offset) = replacement.find("{}") {
        start_char + replacement[..offset + 1].chars().count()
    } else {
        start_char + replacement.chars().count()
    }
}

fn completion_is_environment_context(text: &str, start_char: usize) -> bool {
    let start_byte = nth_char_to_byte(text, start_char);
    text[..start_byte].ends_with("\\begin{")
}

fn is_environment_name(item: &str) -> bool {
    LATEX_ENVIRONMENT_COMPLETIONS.contains(&item)
}

fn nth_char_to_byte(text: &str, char_index: usize) -> usize {
    text.char_indices()
        .nth(char_index)
        .map(|(idx, _)| idx)
        .unwrap_or(text.len())
}

fn start_texlab_client() -> Option<(Sender<TexlabCommand>, Receiver<TexlabEvent>)> {
    let texlab = resolve_command_path("texlab")?;
    let (command_sender, command_receiver) = mpsc::channel();
    let (event_sender, event_receiver) = mpsc::channel();

    std::thread::spawn(move || {
        let result = run_texlab_worker(texlab, command_receiver, event_sender.clone());
        if let Err(err) = result {
            let _ = event_sender.send(TexlabEvent::Error(err));
        }
    });

    Some((command_sender, event_receiver))
}

fn run_texlab_worker(
    texlab: String,
    command_receiver: Receiver<TexlabCommand>,
    event_sender: Sender<TexlabEvent>,
) -> Result<(), String> {
    let mut child = configure_command(Command::new(texlab))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .map_err(|err| format!("Failed to start texlab: {err}"))?;

    let mut stdin = child.stdin.take().ok_or_else(|| "texlab stdin unavailable".to_owned())?;
    let stdout = child.stdout.take().ok_or_else(|| "texlab stdout unavailable".to_owned())?;
    let mut stdout = io::BufReader::new(stdout);
    let mut request_id = 1u64;
    let mut version = 1i32;
    let mut opened_uri: Option<String> = None;

    send_lsp_message(
        &mut stdin,
        &json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": "initialize",
            "params": {
                "processId": std::process::id(),
                "rootUri": Value::Null,
                "capabilities": {}
            }
        }),
    )?;
    let _ = read_lsp_response(&mut stdout, request_id as i64)?;
    request_id += 1;
    send_lsp_message(&mut stdin, &json!({"jsonrpc":"2.0","method":"initialized","params":{}}))?;
    let _ = event_sender.send(TexlabEvent::Ready);

    while let Ok(command) = command_receiver.recv() {
        match command {
            TexlabCommand::SyncDocument { path, text } => {
                sync_texlab_document_state(
                    &mut stdin,
                    &path,
                    &text,
                    &mut opened_uri,
                    &mut version,
                )?;
            }
            TexlabCommand::RequestCompletion {
                path,
                text,
                cursor_char,
                serial,
            } => {
                let uri = sync_texlab_document_state(
                    &mut stdin,
                    &path,
                    &text,
                    &mut opened_uri,
                    &mut version,
                )?;
                let (line, character) = cursor_to_lsp_position(&text, cursor_char);
                let completion_id = request_id;
                request_id += 1;
                send_lsp_message(
                    &mut stdin,
                    &json!({
                        "jsonrpc":"2.0",
                        "id": completion_id,
                        "method":"textDocument/completion",
                        "params":{
                            "textDocument":{"uri":uri},
                            "position":{"line":line,"character":character}
                        }
                    }),
                )?;
                let response = read_lsp_response(&mut stdout, completion_id as i64)?;
                let items = parse_completion_response(&response);
                let _ = event_sender.send(TexlabEvent::Completion { serial, items });
            }
        }
    }

    let _ = child.kill();
    Ok(())
}

fn sync_texlab_document_state(
    stdin: &mut ChildStdin,
    path: &Path,
    text: &str,
    opened_uri: &mut Option<String>,
    version: &mut i32,
) -> Result<String, String> {
    let uri = path_to_file_uri(path);
    if opened_uri.as_deref() != Some(uri.as_str()) {
        *opened_uri = Some(uri.clone());
        *version = 1;
        send_lsp_message(
            stdin,
            &json!({
                "jsonrpc":"2.0",
                "method":"textDocument/didOpen",
                "params":{
                    "textDocument":{
                        "uri":uri,
                        "languageId":"latex",
                        "version":*version,
                        "text":text
                    }
                }
            }),
        )?;
    } else {
        *version += 1;
        send_lsp_message(
            stdin,
            &json!({
                "jsonrpc":"2.0",
                "method":"textDocument/didChange",
                "params":{
                    "textDocument":{
                        "uri":uri,
                        "version":*version
                    },
                    "contentChanges":[{"text":text}]
                }
            }),
        )?;
    }
    Ok(uri)
}

fn send_lsp_message(stdin: &mut ChildStdin, value: &Value) -> Result<(), String> {
    let body = value.to_string();
    write!(stdin, "Content-Length: {}\r\n\r\n{}", body.len(), body)
        .map_err(|err| format!("Failed to write LSP request: {err}"))?;
    stdin.flush().map_err(|err| format!("Failed to flush LSP request: {err}"))
}

fn read_lsp_message(stdout: &mut io::BufReader<ChildStdout>) -> Result<Value, String> {
    let mut content_length = None;
    loop {
        let mut line = String::new();
        stdout
            .read_line(&mut line)
            .map_err(|err| format!("Failed to read LSP header: {err}"))?;
        if line == "\r\n" || line == "\n" {
            break;
        }
        if let Some(value) = line.strip_prefix("Content-Length:") {
            content_length = value.trim().parse::<usize>().ok();
        }
    }

    let content_length = content_length.ok_or_else(|| "Missing LSP content length".to_owned())?;
    let mut body = vec![0u8; content_length];
    std::io::Read::read_exact(stdout, &mut body)
        .map_err(|err| format!("Failed to read LSP body: {err}"))?;
    serde_json::from_slice(&body).map_err(|err| format!("Failed to parse LSP JSON: {err}"))
}

fn read_lsp_response(
    stdout: &mut io::BufReader<ChildStdout>,
    expected_id: i64,
) -> Result<Value, String> {
    loop {
        let message = read_lsp_message(stdout)?;
        if message.get("id").and_then(Value::as_i64) == Some(expected_id) {
            return Ok(message);
        }
    }
}

fn path_to_file_uri(path: &Path) -> String {
    let canonical = path.canonicalize().unwrap_or_else(|_| path.to_path_buf());
    let raw = canonical.to_string_lossy().replace('\\', "/");
    if cfg!(windows) {
        let normalized = if raw.starts_with('/') {
            raw
        } else {
            format!("/{raw}")
        };
        format!("file://{}", percent_encode_file_uri_path(&normalized))
    } else {
        format!("file://{}", percent_encode_file_uri_path(&raw))
    }
}

fn percent_encode_file_uri_path(path: &str) -> String {
    let mut encoded = String::with_capacity(path.len());
    for byte in path.bytes() {
        let is_unreserved = matches!(
            byte,
            b'A'..=b'Z'
                | b'a'..=b'z'
                | b'0'..=b'9'
                | b'-'
                | b'_'
                | b'.'
                | b'~'
                | b'/'
                | b':'
        );
        if is_unreserved {
            encoded.push(byte as char);
        } else {
            encoded.push('%');
            encoded.push_str(&format!("{byte:02X}"));
        }
    }
    encoded
}

fn cursor_to_lsp_position(text: &str, cursor_char: usize) -> (usize, usize) {
    let mut line = 0usize;
    let mut col_utf16 = 0usize;
    for (index, ch) in text.chars().enumerate() {
        if index == cursor_char {
            break;
        }
        if ch == '\n' {
            line += 1;
            col_utf16 = 0;
        } else {
            col_utf16 += ch.len_utf16();
        }
    }
    (line, col_utf16)
}

fn parse_completion_response(response: &Value) -> Vec<LspCompletionItem> {
    let result = response.get("result").unwrap_or(&Value::Null);
    let items = result
        .get("items")
        .and_then(Value::as_array)
        .or_else(|| result.as_array());

    items
        .into_iter()
        .flatten()
        .filter_map(parse_lsp_completion_item)
        .collect()
}

fn parse_lsp_completion_item(item: &Value) -> Option<LspCompletionItem> {
    let label = item.get("label").and_then(Value::as_str)?.to_owned();
    let insert_text = item
        .get("textEdit")
        .and_then(|edit| edit.get("newText"))
        .and_then(Value::as_str)
        .or_else(|| item.get("insertText").and_then(Value::as_str))
        .map(decode_lsp_snippet)
        .unwrap_or_else(|| label.clone());
    let kind = item
        .get("kind")
        .and_then(Value::as_u64)
        .and_then(parse_completion_kind);
    let detail = item
        .get("detail")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(str::to_owned);
    let deprecated = item
        .get("deprecated")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    Some(LspCompletionItem {
        label,
        insert_text,
        kind,
        detail,
        deprecated,
    })
}

fn parse_completion_kind(kind: u64) -> Option<LspCompletionKind> {
    Some(match kind {
        1 => LspCompletionKind::Text,
        2 => LspCompletionKind::Method,
        3 => LspCompletionKind::Function,
        4 => LspCompletionKind::Constructor,
        5 => LspCompletionKind::Field,
        6 => LspCompletionKind::Variable,
        7 => LspCompletionKind::Class,
        8 => LspCompletionKind::Interface,
        9 => LspCompletionKind::Module,
        10 => LspCompletionKind::Property,
        11 => LspCompletionKind::Unit,
        12 => LspCompletionKind::Value,
        13 => LspCompletionKind::Enum,
        14 => LspCompletionKind::Keyword,
        15 => LspCompletionKind::Snippet,
        16 => LspCompletionKind::Color,
        17 => LspCompletionKind::File,
        18 => LspCompletionKind::Reference,
        19 => LspCompletionKind::Folder,
        20 => LspCompletionKind::EnumMember,
        21 => LspCompletionKind::Constant,
        22 => LspCompletionKind::Struct,
        23 => LspCompletionKind::Event,
        24 => LspCompletionKind::Operator,
        25 => LspCompletionKind::TypeParameter,
        _ => return None,
    })
}

fn decode_lsp_snippet(text: &str) -> String {
    let mut out = String::new();
    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        if chars[i] == '$' {
            if i + 1 < chars.len() && chars[i + 1] == '0' {
                i += 2;
                continue;
            }
            if i + 1 < chars.len() && chars[i + 1] == '{' {
                i += 2;
                while i < chars.len() && chars[i].is_ascii_digit() {
                    i += 1;
                }
                if i < chars.len() && chars[i] == ':' {
                    i += 1;
                    while i < chars.len() && chars[i] != '}' {
                        out.push(chars[i]);
                        i += 1;
                    }
                } else {
                    while i < chars.len() && chars[i] != '}' {
                        i += 1;
                    }
                }
                if i < chars.len() && chars[i] == '}' {
                    i += 1;
                }
                continue;
            }
        }
        out.push(chars[i]);
        i += 1;
    }
    out
}

fn completion_match_text(item: &LspCompletionItem) -> &str {
    if item.insert_text.starts_with('\\') {
        &item.insert_text
    } else {
        &item.label
    }
}

fn completion_kind_badge(item: &LspCompletionItem, environment_context: bool) -> (&'static str, Color32) {
    let text = completion_match_text(item);
    if environment_context || text.starts_with("\\begin") || text.starts_with("\\end") {
        return ("env", Color32::from_rgb(86, 211, 194));
    }
    if text.starts_with('\\') {
        return ("cmd", Color32::from_rgb(88, 166, 255));
    }
    match item.kind {
        Some(LspCompletionKind::Snippet) => ("snp", Color32::from_rgb(180, 120, 255)),
        Some(LspCompletionKind::Keyword | LspCompletionKind::Operator) => {
            ("kw", Color32::from_rgb(255, 166, 87))
        }
        Some(LspCompletionKind::File | LspCompletionKind::Folder) => {
            ("fs", Color32::from_rgb(120, 200, 140))
        }
        Some(LspCompletionKind::Class | LspCompletionKind::Struct | LspCompletionKind::Interface) => {
            ("typ", Color32::from_rgb(86, 211, 194))
        }
        Some(LspCompletionKind::Variable | LspCompletionKind::Field | LspCompletionKind::Property) => {
            ("var", Color32::from_rgb(229, 192, 123))
        }
        Some(LspCompletionKind::Function | LspCompletionKind::Method | LspCompletionKind::Constructor) => {
            ("fn", Color32::from_rgb(88, 166, 255))
        }
        _ => ("txt", Color32::from_rgb(140, 140, 140)),
    }
}

fn completion_label_color(item: &LspCompletionItem, environment_context: bool) -> Color32 {
    let text = completion_match_text(item);
    if environment_context || text.starts_with("\\begin") || text.starts_with("\\end") {
        return Color32::from_rgb(126, 230, 214);
    }
    if text.starts_with('\\') {
        return Color32::from_rgb(132, 201, 255);
    }
    match item.kind {
        Some(LspCompletionKind::Keyword | LspCompletionKind::Operator) => {
            Color32::from_rgb(255, 196, 122)
        }
        Some(LspCompletionKind::Snippet) => Color32::from_rgb(212, 168, 255),
        Some(LspCompletionKind::File | LspCompletionKind::Folder) => {
            Color32::from_rgb(152, 224, 168)
        }
        Some(LspCompletionKind::Class | LspCompletionKind::Struct | LspCompletionKind::Interface) => {
            Color32::from_rgb(126, 230, 214)
        }
        Some(LspCompletionKind::Variable | LspCompletionKind::Field | LspCompletionKind::Property) => {
            Color32::from_rgb(240, 210, 138)
        }
        Some(LspCompletionKind::Function | LspCompletionKind::Method | LspCompletionKind::Constructor) => {
            Color32::from_rgb(132, 201, 255)
        }
        _ => Color32::from_rgb(232, 232, 232),
    }
}

fn completion_detail_preview(item: &LspCompletionItem) -> Option<String> {
    let detail = item.detail.as_ref()?;
    let trimmed = detail.trim();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.chars().count() > 28 {
        let shortened: String = trimmed.chars().take(28).collect();
        Some(format!("{shortened}..."))
    } else {
        Some(trimmed.to_owned())
    }
}

fn read_directory_entries(root: &Path) -> Vec<fs::DirEntry> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };

    let mut entries: Vec<_> = entries.flatten().collect();
    entries.sort_by(|a, b| compare_dir_entries(a, b));
    entries
}

fn build_file_node(path: &Path) -> FileNode {
    let is_dir = path.is_dir();
    let children = if is_dir {
        read_directory_entries(path)
            .into_iter()
            .filter(|entry| !should_skip_in_file_tree(&entry.path()))
            .map(|entry| build_file_node(&entry.path()))
            .collect()
    } else {
        Vec::new()
    };

    FileNode {
        path: path.to_path_buf(),
        is_dir,
        children,
    }
}

fn compare_dir_entries(a: &fs::DirEntry, b: &fs::DirEntry) -> std::cmp::Ordering {
    let a_is_dir = a.file_type().map(|kind| kind.is_dir()).unwrap_or(false);
    let b_is_dir = b.file_type().map(|kind| kind.is_dir()).unwrap_or(false);
    match b_is_dir.cmp(&a_is_dir) {
        std::cmp::Ordering::Equal => a
            .file_name()
            .to_string_lossy()
            .to_ascii_lowercase()
            .cmp(&b.file_name().to_string_lossy().to_ascii_lowercase()),
        other => other,
    }
}

fn should_skip_in_file_tree(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| matches!(name, "target" | ".git"))
        .unwrap_or(false)
}

fn unique_destination_path(dir: &Path, file_name: &std::ffi::OsStr) -> PathBuf {
    let initial = dir.join(file_name);
    if !initial.exists() {
        return initial;
    }

    let stem = Path::new(file_name)
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("copy");
    let ext = Path::new(file_name).extension().and_then(|s| s.to_str());

    for index in 1..1000 {
        let candidate_name = match ext {
            Some(ext) => format!("{stem}_copy{index}.{ext}"),
            None => format!("{stem}_copy{index}"),
        };
        let candidate = dir.join(candidate_name);
        if !candidate.exists() {
            return candidate;
        }
    }

    dir.join(file_name)
}

fn show_tree_context_menu(
    ui: &mut egui::Ui,
    target_dir: &Path,
    file_new_name: &mut String,
    can_paste: bool,
    action: &mut Option<TreeAction>,
    allow_create: bool,
) {
    if allow_create {
        ui.set_min_width(220.0);
        ui.label("name");
        ui.text_edit_singleline(file_new_name);
        let has_name = !file_new_name.trim().is_empty();
        if ui.add_enabled(has_name, egui::Button::new("new file")).clicked() {
            *action = Some(TreeAction::NewFile(target_dir.to_path_buf()));
            ui.close_menu();
        }
        if ui
            .add_enabled(has_name, egui::Button::new("new folder"))
            .clicked()
        {
            *action = Some(TreeAction::NewFolder(target_dir.to_path_buf()));
            ui.close_menu();
        }
        ui.separator();
    }

    if ui
        .add_enabled(can_paste, egui::Button::new("paste"))
        .clicked()
    {
        *action = Some(TreeAction::Paste(target_dir.to_path_buf()));
        ui.close_menu();
    }
}

fn copy_path_recursively(source: &Path, destination: &Path) -> io::Result<()> {
    if source.is_dir() {
        fs::create_dir_all(destination)?;
        for entry in fs::read_dir(source)? {
            let entry = entry?;
            copy_path_recursively(&entry.path(), &destination.join(entry.file_name()))?;
        }
        Ok(())
    } else {
        if let Some(parent) = destination.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::copy(source, destination)?;
        Ok(())
    }
}

fn remove_path_recursively(path: &Path) -> io::Result<()> {
    if path.is_dir() {
        fs::remove_dir_all(path)
    } else {
        fs::remove_file(path)
    }
}

fn is_tex_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()).map(|ext| ext.to_ascii_lowercase()),
        Some(ext) if matches!(ext.as_str(), "tex" | "cls" | "sty" | "bib")
    )
}

fn is_editor_text_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()).map(|ext| ext.to_ascii_lowercase()),
        Some(ext)
            if matches!(
                ext.as_str(),
                "tex" | "cls" | "sty" | "bib" | "log" | "txt" | "md" | "csv" | "json" | "yaml" | "yml" | "toml"
            )
    )
}

fn is_read_only_editor_path(path: &Path) -> bool {
    path.extension()
        .and_then(|ext| ext.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("log"))
        .unwrap_or(false)
}

fn tree_item_icon(path: &Path, is_dir: bool) -> &'static str {
    if is_dir {
        "📁"
    } else {
        match path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.to_ascii_lowercase())
            .as_deref()
        {
            Some("tex") => "📘",
            Some("log") => "🧾",
            Some("bib") => "📚",
            Some("sty") | Some("cls") => "🧩",
            Some("pdf") => "📕",
            Some("png") | Some("jpg") | Some("jpeg") | Some("svg") => "🖼",
            _ => "📄",
        }
    }
}

fn default_file_name(current: Option<&Path>) -> &'static str {
    if current.is_some() {
        "document.tex"
    } else {
        "untitled.tex"
    }
}

fn math_preview_at_cursor(text: &str, cursor_char: usize) -> Option<MathPreview> {
    find_math_spans(text)
        .into_iter()
        .find(|span| span.start_char <= cursor_char && cursor_char <= span.end_char)
        .map(|span| MathPreview {
            mode: span.mode,
            source: span.source,
            line: span.line,
        })
}

fn find_math_spans(text: &str) -> Vec<MathSpan> {
    let bytes = text.as_bytes();
    let mut spans = Vec::new();
    let mut i = 0usize;

    while i < bytes.len() {
        if bytes[i] == b'%' && !is_escaped(bytes, i) {
            while i < bytes.len() && bytes[i] != b'\n' {
                i += 1;
            }
            continue;
        }

        if bytes[i..].starts_with(br"\[") {
            if let Some(end) = find_unescaped_pattern(bytes, i + 2, br"\]") {
                spans.push(make_math_span(text, i, end + 2, i + 2, end, "display \\[...\\]"));
                i = end + 2;
                continue;
            }
        }

        if bytes[i..].starts_with(br"\(") {
            if let Some(end) = find_unescaped_pattern(bytes, i + 2, br"\)") {
                spans.push(make_math_span(text, i, end + 2, i + 2, end, "inline \\(...\\)"));
                i = end + 2;
                continue;
            }
        }

        if bytes[i..].starts_with(br"\begin{") {
            if let Some(name_end_offset) = bytes[i + 7..].iter().position(|b| *b == b'}') {
                let name_start = i + 7;
                let name_end = name_start + name_end_offset;
                let env_name = &text[name_start..name_end];
                if is_math_environment(env_name) {
                    let end_marker = format!("\\end{{{env_name}}}");
                    if let Some(close_start_rel) = text[name_end + 1..].find(&end_marker) {
                        let close_start = name_end + 1 + close_start_rel;
                        spans.push(make_math_span(
                            text,
                            i,
                            close_start + end_marker.len(),
                            name_end + 1,
                            close_start,
                            &format!("environment {env_name}"),
                        ));
                        i = close_start + end_marker.len();
                        continue;
                    }
                }
            }
        }

        if bytes[i] == b'$' && !is_escaped(bytes, i) {
            if i + 1 < bytes.len() && bytes[i + 1] == b'$' {
                if let Some(end) = find_double_dollar(bytes, i + 2) {
                    spans.push(make_math_span(text, i, end + 2, i + 2, end, "display $$...$$"));
                    i = end + 2;
                    continue;
                }
            } else if let Some(end) = find_single_dollar(bytes, i + 1) {
                spans.push(make_math_span(text, i, end + 1, i + 1, end, "inline $...$"));
                i = end + 1;
                continue;
            }
        }

        i += 1;
    }

    spans
}

fn make_math_span(
    text: &str,
    start_byte: usize,
    end_byte: usize,
    source_start: usize,
    source_end: usize,
    mode: &str,
) -> MathSpan {
    MathSpan {
        start_char: text[..start_byte].chars().count(),
        end_char: text[..end_byte].chars().count(),
        start_byte,
        end_byte,
        source: text[source_start..source_end].trim().to_owned(),
        mode: mode.to_owned(),
        line: text[..start_byte].bytes().filter(|b| *b == b'\n').count() + 1,
    }
}

fn is_math_environment(name: &str) -> bool {
    matches!(
        name,
        "equation"
            | "equation*"
            | "align"
            | "align*"
            | "gather"
            | "gather*"
            | "multline"
            | "multline*"
            | "displaymath"
            | "math"
    )
}

fn find_unescaped_pattern(bytes: &[u8], start: usize, pattern: &[u8]) -> Option<usize> {
    let mut i = start;
    while i + pattern.len() <= bytes.len() {
        if bytes[i..].starts_with(pattern) && !is_escaped(bytes, i) {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn find_single_dollar(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i < bytes.len() {
        if bytes[i] == b'\n' {
            return None;
        }
        if bytes[i] == b'$'
            && !is_escaped(bytes, i)
            && (i + 1 >= bytes.len() || bytes[i + 1] != b'$')
        {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn find_double_dollar(bytes: &[u8], start: usize) -> Option<usize> {
    let mut i = start;
    while i + 1 < bytes.len() {
        if bytes[i] == b'$' && bytes[i + 1] == b'$' && !is_escaped(bytes, i) {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn is_escaped(bytes: &[u8], index: usize) -> bool {
    if index == 0 {
        return false;
    }
    let mut count = 0usize;
    let mut i = index;
    while i > 0 {
        i -= 1;
        if bytes[i] == b'\\' {
            count += 1;
        } else {
            break;
        }
    }
    count % 2 == 1
}

fn analyze_tex(text: &str, cursor_line: Option<usize>, settings: &AppSettings) -> TexAnalysis {
    let mut analysis = TexAnalysis {
        line_count: text.lines().count().max(1),
        char_count: text.chars().count(),
        ..Default::default()
    };

    let mut env_stack: Vec<(String, usize)> = Vec::new();
    let mut brace_stack: Vec<usize> = Vec::new();
    let mut labels: Vec<(String, usize)> = Vec::new();
    let mut refs: Vec<(String, usize)> = Vec::new();

    for (line_idx, raw_line) in text.lines().enumerate() {
        let line_no = line_idx + 1;
        let line = strip_comment(raw_line);

        collect_outline(&line, line_no, &mut analysis.outline);
        collect_commands(
            &line,
            "\\label{",
            "label",
            line_no,
            &mut labels,
            &mut analysis.symbols,
        );
        collect_commands(
            &line,
            "\\ref{",
            "ref",
            line_no,
            &mut refs,
            &mut analysis.symbols,
        );
        collect_commands(
            &line,
            "\\eqref{",
            "eqref",
            line_no,
            &mut refs,
            &mut analysis.symbols,
        );
        collect_commands(
            &line,
            "\\cite{",
            "cite",
            line_no,
            &mut refs,
            &mut analysis.symbols,
        );
        collect_environment_markers(&line, line_no, &mut env_stack, &mut analysis.diagnostics);
        collect_braces(&line, line_no, &mut brace_stack, &mut analysis.diagnostics);
    }

    analysis.label_count = labels.len();
    analysis.reference_count = refs.len();

    for (env, line) in env_stack {
        analysis.diagnostics.push(Diagnostic {
            kind: DiagnosticKind::General,
            severity: Severity::Error,
            line,
            message: format!("Environment `{env}` is not closed"),
            start_char: None,
            end_char: None,
        });
    }

    for line in brace_stack {
        analysis.diagnostics.push(Diagnostic {
            kind: DiagnosticKind::General,
            severity: Severity::Error,
            line,
            message: "Opening brace `{` is not closed".to_owned(),
            start_char: None,
            end_char: None,
        });
    }

    for (reference, line) in refs {
        if reference.contains(',') {
            for entry in reference.split(',').map(str::trim).filter(|s| !s.is_empty()) {
                if !labels.iter().any(|(label, _)| label == entry) {
                    analysis.diagnostics.push(Diagnostic {
                        kind: DiagnosticKind::General,
                        severity: Severity::Warning,
                        line,
                        message: format!("Reference `{entry}` has no matching label"),
                        start_char: None,
                        end_char: None,
                    });
                }
            }
        } else if !labels.iter().any(|(label, _)| label == &reference) {
            analysis.diagnostics.push(Diagnostic {
                kind: DiagnosticKind::General,
                severity: Severity::Warning,
                line,
                message: format!("Reference `{reference}` has no matching label"),
                start_char: None,
                end_char: None,
            });
        }
    }

    if !text.contains("\\begin{document}") {
        analysis.diagnostics.push(Diagnostic {
            kind: DiagnosticKind::General,
            severity: Severity::Info,
            line: 1,
            message: "Document body marker `\\begin{document}` was not found".to_owned(),
            start_char: None,
            end_char: None,
        });
    }

    collect_spelling_diagnostics(text, cursor_line, settings, &mut analysis.diagnostics);
    fill_error_diagnostic_ranges(text, &mut analysis.diagnostics);
    analysis
}

fn fill_error_diagnostic_ranges(text: &str, diagnostics: &mut [Diagnostic]) {
    for diagnostic in diagnostics {
        if diagnostic.severity != Severity::Error || diagnostic.start_char.is_some() {
            continue;
        }
        if let Some((start_char, end_char)) = line_content_char_range(text, diagnostic.line) {
            diagnostic.start_char = Some(start_char);
            diagnostic.end_char = Some(end_char);
        }
    }
}

fn collect_outline(line: &str, line_no: usize, outline: &mut Vec<OutlineItem>) {
    const COMMANDS: [(&str, usize); 7] = [
        ("\\part{", 1),
        ("\\chapter{", 1),
        ("\\section{", 1),
        ("\\subsection{", 2),
        ("\\subsubsection{", 3),
        ("\\paragraph{", 4),
        ("\\subparagraph{", 5),
    ];

    for (command, level) in COMMANDS {
        let mut offset = 0;
        while let Some(pos) = line[offset..].find(command) {
            let start = offset + pos + command.len();
            if let Some(end) = find_closing_brace(line, start) {
                outline.push(OutlineItem {
                    command: command.trim_end_matches('{').to_owned(),
                    title: line[start..end].trim().to_owned(),
                    level,
                    line: line_no,
                });
                offset = end + 1;
            } else {
                break;
            }
        }
    }
}

fn collect_commands(
    line: &str,
    command: &str,
    kind: &str,
    line_no: usize,
    values: &mut Vec<(String, usize)>,
    symbols: &mut Vec<SymbolItem>,
) {
    let mut offset = 0;
    while let Some(pos) = line[offset..].find(command) {
        let start = offset + pos + command.len();
        if let Some(end) = find_closing_brace(line, start) {
            let name = line[start..end].trim().to_owned();
            values.push((name.clone(), line_no));
            symbols.push(SymbolItem {
                kind: kind.to_owned(),
                name,
                line: line_no,
            });
            offset = end + 1;
        } else {
            break;
        }
    }
}

fn collect_environment_markers(
    line: &str,
    line_no: usize,
    env_stack: &mut Vec<(String, usize)>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut offset = 0;
    while offset < line.len() {
        let begin_pos = line[offset..].find("\\begin{");
        let end_pos = line[offset..].find("\\end{");

        let (command, pos) = match (begin_pos, end_pos) {
            (Some(a), Some(b)) if a <= b => ("begin", offset + a),
            (Some(_), Some(b)) => ("end", offset + b),
            (Some(a), None) => ("begin", offset + a),
            (None, Some(b)) => ("end", offset + b),
            (None, None) => break,
        };

        let name_start = pos + if command == "begin" { 7 } else { 5 };
        if let Some(name_end) = find_closing_brace(line, name_start) {
            let env_name = line[name_start..name_end].trim().to_owned();
            if command == "begin" {
                env_stack.push((env_name, line_no));
            } else if let Some((open_name, _)) = env_stack.pop() {
                if open_name != env_name {
                    diagnostics.push(Diagnostic {
                        kind: DiagnosticKind::General,
                        severity: Severity::Error,
                        line: line_no,
                        message: format!(
                            "Environment mismatch: expected `\\end{{{open_name}}}`, found `\\end{{{env_name}}}`"
                        ),
                        start_char: None,
                        end_char: None,
                    });
                }
            } else {
                diagnostics.push(Diagnostic {
                    kind: DiagnosticKind::General,
                    severity: Severity::Error,
                    line: line_no,
                    message: format!("Unexpected `\\end{{{env_name}}}`"),
                    start_char: None,
                    end_char: None,
                });
            }
            offset = name_end + 1;
        } else {
            break;
        }
    }
}

fn collect_braces(
    line: &str,
    line_no: usize,
    brace_stack: &mut Vec<usize>,
    diagnostics: &mut Vec<Diagnostic>,
) {
    let mut escaped = false;
    for ch in line.chars() {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '{' {
            brace_stack.push(line_no);
        } else if ch == '}' && brace_stack.pop().is_none() {
            diagnostics.push(Diagnostic {
                kind: DiagnosticKind::General,
                severity: Severity::Error,
                line: line_no,
                message: "Closing brace `}` has no matching opening brace".to_owned(),
                start_char: None,
                end_char: None,
            });
        }
    }
}

fn collect_spelling_diagnostics(
    text: &str,
    cursor_line: Option<usize>,
    settings: &AppSettings,
    diagnostics: &mut Vec<Diagnostic>,
) {
    if !settings.spellcheck_enabled {
        return;
    }

    let tokens = collect_spelling_tokens(text);
    if tokens.is_empty() {
        return;
    }

    let misspelled = if let Some(config) = resolved_hunspell_config(settings.preferred_dictionary.as_deref()) {
        hunspell_misspellings(&config, &tokens)
            .or_else(|| fallback_dictionary_misspellings(settings.preferred_dictionary.as_deref(), &tokens))
    } else {
        fallback_dictionary_misspellings(settings.preferred_dictionary.as_deref(), &tokens)
    };

    let Some(misspelled) = misspelled else {
        return;
    };
    let fallback_dictionary = english_dictionary();

    let mut seen = HashSet::new();
    let mut tokens = tokens;
    if let Some(cursor_line) = cursor_line {
        tokens.sort_by_key(|(line, ..)| line.abs_diff(cursor_line));
    }

    for (line, start_char, end_char, token) in tokens {
        if fallback_dictionary
            .map(|dictionary| is_known_english_word(&token, dictionary))
            .unwrap_or(false)
        {
            continue;
        }
        if misspelled.contains(&token) && seen.insert((line, token.clone())) {
            diagnostics.push(Diagnostic {
                kind: DiagnosticKind::Spelling,
                severity: Severity::Warning,
                line,
                message: format!("Possible spelling mistake: `{token}`"),
                start_char: Some(start_char),
                end_char: Some(end_char),
            });
            if seen.len() >= 20 {
                break;
            }
        }
    }
}

fn sanitize_text_for_spellcheck(text: &str) -> String {
    let mut bytes = text.as_bytes().to_vec();

    for span in find_math_spans(text) {
        for byte in &mut bytes[span.start_byte..span.end_byte] {
            if *byte != b'\n' {
                *byte = b' ';
            }
        }
    }

    let mut i = 0usize;
    while i < bytes.len() {
        if bytes[i] == b'%' && !is_escaped(&bytes, i) {
            while i < bytes.len() && bytes[i] != b'\n' {
                bytes[i] = b' ';
                i += 1;
            }
            continue;
        }

        if bytes[i] == b'\\' && !is_escaped(&bytes, i) {
            bytes[i] = b' ';
            i += 1;
            while i < bytes.len() && ((bytes[i] as char).is_ascii_alphabetic() || bytes[i] == b'@') {
                bytes[i] = b' ';
                i += 1;
            }
            if i < bytes.len() && bytes[i].is_ascii_punctuation() && bytes[i] != b'{' && bytes[i] != b'}' {
                bytes[i] = b' ';
                i += 1;
            }
            continue;
        }

        i += 1;
    }

    String::from_utf8_lossy(&bytes).into_owned()
}

fn collect_spelling_tokens(text: &str) -> Vec<(usize, usize, usize, String)> {
    let sanitized = sanitize_text_for_spellcheck(text);
    let mut tokens = Vec::new();
    let mut line = 1usize;
    let mut token = String::new();
    let mut token_line = line;
    let mut token_start_char = 0usize;
    let mut global_char = 0usize;

    for ch in sanitized.chars().chain(std::iter::once(' ')) {
        if ch == '\n' {
            if let Some(token) = normalize_spelling_candidate(&token) {
                tokens.push((token_line, token_start_char, global_char, token));
            }
            token.clear();
            line += 1;
            global_char += 1;
            continue;
        }

        if ch.is_ascii_alphabetic() || (ch == '\'' && !token.is_empty()) {
            if token.is_empty() {
                token_line = line;
                token_start_char = global_char;
            }
            token.push(ch);
        } else if !token.is_empty() {
            if let Some(token) = normalize_spelling_candidate(&token) {
                tokens.push((token_line, token_start_char, global_char, token));
            }
            token.clear();
        }
        global_char += 1;
    }

    tokens
}

fn line_start_char(text: &str, line: usize) -> usize {
    if line <= 1 {
        return 0;
    }

    let mut current_line = 1usize;
    for (index, ch) in text.chars().enumerate() {
        if current_line == line {
            return index;
        }
        if ch == '\n' {
            current_line += 1;
        }
    }
    text.chars().count()
}

fn char_index_to_line(text: &str, char_index: usize) -> usize {
    text.chars()
        .take(char_index)
        .filter(|ch| *ch == '\n')
        .count()
        + 1
}

fn line_content_char_range(text: &str, line: usize) -> Option<(usize, usize)> {
    let start_char = line_start_char(text, line);
    let line_text = text.lines().nth(line.saturating_sub(1))?;
    let leading = line_text
        .chars()
        .take_while(|ch| ch.is_whitespace())
        .count();
    let trailing = line_text
        .chars()
        .rev()
        .take_while(|ch| ch.is_whitespace())
        .count();
    let visible_len = line_text.chars().count().saturating_sub(leading + trailing);
    if visible_len == 0 {
        return None;
    }
    Some((start_char + leading, start_char + leading + visible_len))
}

fn paint_line_number_gutter(
    ui: &egui::Ui,
    gutter_rect: egui::Rect,
    output: &egui::text_edit::TextEditOutput,
    line_count: usize,
) {
    let painter = ui.painter();
    let visuals = ui.visuals();
    painter.line_segment(
        [
            egui::pos2(gutter_rect.right(), gutter_rect.top()),
            egui::pos2(gutter_rect.right(), gutter_rect.bottom()),
        ],
        egui::Stroke::new(1.0, visuals.widgets.noninteractive.bg_stroke.color),
    );

    let font_id = egui::TextStyle::Monospace.resolve(ui.style());
    let text_color = visuals.weak_text_color();
    let mut line_number = 1usize;

    for row in &output.galley.rows {
        let row_rect = row.rect.translate(output.galley_pos.to_vec2());
        let pos = egui::pos2(gutter_rect.right() - 8.0, row_rect.center().y);
        painter.text(
            pos,
            egui::Align2::RIGHT_CENTER,
            line_number.to_string(),
            font_id.clone(),
            text_color,
        );

        if row.ends_with_newline {
            line_number += 1;
        }
    }

    if line_number == 1 && line_count == 0 {
        painter.text(
            egui::pos2(gutter_rect.right() - 8.0, gutter_rect.top() + 10.0),
            egui::Align2::RIGHT_TOP,
            "1",
            font_id,
            text_color,
        );
    }
}

fn normalize_spelling_candidate(token: &str) -> Option<String> {
    let normalized = token.trim_matches('\'').to_ascii_lowercase();
    (normalized.len() >= 4).then_some(normalized)
}

fn is_known_english_word(word: &str, dictionary: &HashSet<String>) -> bool {
    dictionary.contains(word)
        || dictionary.contains(&format!("{word}s"))
        || dictionary.contains(&format!("{word}ed"))
        || dictionary.contains(&format!("{word}ing"))
}

fn fallback_dictionary_misspellings(
    preferred_dictionary: Option<&str>,
    tokens: &[(usize, usize, usize, String)],
) -> Option<HashSet<String>> {
    if preferred_dictionary
        .map(|name| !name.starts_with("en_"))
        .unwrap_or(false)
    {
        return None;
    }
    let dictionary = english_dictionary()?;
    Some(
        tokens
            .iter()
            .map(|(_, _, _, token)| token)
            .filter(|token| !is_known_english_word(token, dictionary))
            .cloned()
            .collect(),
    )
}

fn hunspell_misspellings(
    config: &HunspellConfig,
    tokens: &[(usize, usize, usize, String)],
) -> Option<HashSet<String>> {
    let unique_tokens: Vec<_> = tokens
        .iter()
        .map(|(_, _, _, token)| token.clone())
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();

    if unique_tokens.is_empty() {
        return Some(HashSet::new());
    }

    let mut command = configure_command(Command::new(&config.command_path));
    command.args(["-d", &config.dictionary_name, "-l"]);
    if let Some(dict_dir) = &config.dictionary_dir {
        command.env("DICPATH", dict_dir);
    }

    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .ok()?;

    if let Some(stdin) = &mut child.stdin {
        let input = format!("{}\n", unique_tokens.join("\n"));
        if stdin.write_all(input.as_bytes()).is_err() {
            return None;
        }
    }

    let output = child.wait_with_output().ok()?;
    if !output.status.success() {
        return None;
    }

    Some(
        String::from_utf8_lossy(&output.stdout)
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(|line| line.to_ascii_lowercase())
            .collect(),
    )
}

fn english_dictionary() -> Option<&'static HashSet<String>> {
    static DICTIONARY: OnceLock<Option<HashSet<String>>> = OnceLock::new();
    DICTIONARY
        .get_or_init(load_english_dictionary)
        .as_ref()
}

struct HunspellConfig {
    command_path: String,
    dictionary_name: String,
    dictionary_dir: Option<String>,
}

fn available_hunspell_dictionaries() -> &'static [HunspellDictionary] {
    static DICTIONARIES: OnceLock<Vec<HunspellDictionary>> = OnceLock::new();
    DICTIONARIES.get_or_init(discover_hunspell_dictionaries).as_slice()
}

fn discover_hunspell_dictionaries() -> Vec<HunspellDictionary> {
    let mut seen = HashSet::new();
    let mut dictionaries = Vec::new();

    for dir in known_hunspell_dictionary_directories() {
        let Ok(entries) = fs::read_dir(dir) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|ext| ext.to_str()) != Some("dic") {
                continue;
            }
            let Some(name) = path.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            if seen.insert(name.to_owned()) {
                dictionaries.push(HunspellDictionary {
                    name: name.to_owned(),
                    path,
                });
            }
        }
    }

    dictionaries.sort_by(|a, b| a.name.cmp(&b.name));
    dictionaries
}

fn known_hunspell_dictionary_directories() -> Vec<PathBuf> {
    let mut dirs = bundled_hunspell_dictionary_directories();
    dirs.extend([
        PathBuf::from("/usr/share/hunspell"),
        PathBuf::from("/usr/share/myspell"),
        PathBuf::from(r"C:\Program Files\Hunspell\share\hunspell"),
        PathBuf::from(r"C:\Program Files\Hunspell"),
    ]);
    dirs
}

fn bundled_hunspell_dictionary_directories() -> Vec<PathBuf> {
    let mut dirs = Vec::new();
    for root in app_search_roots() {
        dirs.push(root.clone());
        dirs.push(root.join("dict"));
        dirs.push(root.join("hunspell"));
        dirs.push(root.join("hunspell").join("dict"));
        dirs.push(root.join("dist-win64"));
        dirs.push(root.join("hunspell").join("dist-win64"));
        dirs.push(root.join("tools").join("hunspell"));
        dirs.push(root.join("tools").join("hunspell").join("dict"));
        dirs.push(root.join("tools").join("hunspell").join("dist-win64"));
    }
    dirs
}

fn legacy_hunspell_dictionary_paths(name: &str) -> Vec<PathBuf> {
    known_hunspell_dictionary_directories()
        .into_iter()
        .map(|dir| dir.join(format!("{name}.dic")))
        .collect()
}

fn dictionary_display_label(name: &str) -> String {
    match name {
        "Auto" => "Auto".to_owned(),
        "en_US" => "English (US)".to_owned(),
        "en_GB" => "English (UK)".to_owned(),
        "ja_JP" => "Japanese".to_owned(),
        "de_DE" => "German".to_owned(),
        "fr_FR" => "French".to_owned(),
        "es_ES" => "Spanish".to_owned(),
        "it_IT" => "Italian".to_owned(),
        "pt_BR" => "Portuguese (Brazil)".to_owned(),
        "pt_PT" => "Portuguese (Portugal)".to_owned(),
        _ => name.replace('_', "-"),
    }
}

fn resolved_hunspell_config(preferred_dictionary: Option<&str>) -> Option<HunspellConfig> {
    let command_path = resolve_command_path("hunspell")?;
    let dictionary = preferred_dictionary
        .and_then(|name| {
            available_hunspell_dictionaries()
                .iter()
                .find(|dictionary| dictionary.name == name)
        })
        .or_else(|| available_hunspell_dictionaries().first())?;

    let dictionary_dir = dictionary
        .path
        .parent()
        .map(|path| path.to_string_lossy().into_owned());

    Some(HunspellConfig {
        command_path,
        dictionary_name: dictionary.name.clone(),
        dictionary_dir,
    })
}

fn load_english_dictionary() -> Option<HashSet<String>> {
    let dictionary_path = known_english_dictionary_paths()
        .into_iter()
        .find(|path| path.exists())?;
    let content = fs::read_to_string(dictionary_path).ok()?;
    let mut words = HashSet::new();

    for line in content.lines().skip(1) {
        let word = line.split('/').next().unwrap_or_default().trim().to_ascii_lowercase();
        if !word.is_empty() && word.chars().all(|ch| ch.is_ascii_alphabetic() || ch == '\'') {
            words.insert(word);
        }
    }

    Some(words)
}

fn known_english_dictionary_paths() -> Vec<PathBuf> {
    let mut paths: Vec<_> = available_hunspell_dictionaries()
        .iter()
        .filter(|dictionary| dictionary.name == "en_US")
        .map(|dictionary| dictionary.path.clone())
        .collect();
    paths.extend(legacy_hunspell_dictionary_paths("en_US"));
    paths
}

fn app_search_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(exe) = std::env::current_exe() {
        if let Some(dir) = exe.parent() {
            roots.push(dir.to_path_buf());
        }
    }
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd);
    }
    roots.push(PathBuf::from(env!("CARGO_MANIFEST_DIR")));
    roots
}

fn strip_comment(line: &str) -> String {
    let mut out = String::new();
    let mut escaped = false;
    for ch in line.chars() {
        if escaped {
            out.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            out.push(ch);
            escaped = true;
            continue;
        }
        if ch == '%' {
            break;
        }
        out.push(ch);
    }
    out
}

fn find_closing_brace(line: &str, start: usize) -> Option<usize> {
    let mut depth = 1usize;
    let mut escaped = false;

    for (idx, ch) in line.char_indices().skip_while(|(idx, _)| *idx < start) {
        if escaped {
            escaped = false;
            continue;
        }
        if ch == '\\' {
            escaped = true;
            continue;
        }
        if ch == '{' {
            depth += 1;
        } else if ch == '}' {
            depth = depth.saturating_sub(1);
            if depth == 0 {
                return Some(idx);
            }
        }
    }
    None
}

fn highlight_tex(text: &str, diagnostics: &[Diagnostic]) -> LayoutJob {
    let mut job = LayoutJob::default();
    let default = TextFormat {
        font_id: egui::FontId::monospace(14.0),
        color: Color32::from_rgb(220, 220, 220),
        ..Default::default()
    };
    let comment = TextFormat {
        font_id: egui::FontId::monospace(14.0),
        color: Color32::from_rgb(110, 160, 110),
        ..Default::default()
    };
    let command = TextFormat {
        font_id: egui::FontId::monospace(14.0),
        color: Color32::from_rgb(120, 180, 250),
        ..Default::default()
    };
    let brace = TextFormat {
        font_id: egui::FontId::monospace(14.0),
        color: Color32::from_rgb(220, 170, 80),
        ..Default::default()
    };
    let math = TextFormat {
        font_id: egui::FontId::monospace(14.0),
        color: Color32::from_rgb(220, 120, 170),
        ..Default::default()
    };

    let chars: Vec<char> = text.chars().collect();
    let mut i = 0usize;
    let mut in_inline_math = false;

    while i < chars.len() {
        let ch = chars[i];

        if ch == '%' {
            let start = i;
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
            append_chars(&mut job, &chars[start..i], comment.clone());
            continue;
        }

        if ch == '\\' {
            let start = i;
            i += 1;
            while i < chars.len() && (chars[i].is_alphabetic() || chars[i] == '@') {
                i += 1;
            }
            if i == start + 1 && i < chars.len() {
                i += 1;
            }
            append_chars(&mut job, &chars[start..i], command.clone());
            continue;
        }

        if ch == '$' {
            let start = i;
            i += 1;
            if i < chars.len() && chars[i] == '$' {
                i += 1;
            } else {
                in_inline_math = !in_inline_math;
            }
            append_chars(&mut job, &chars[start..i], math.clone());
            continue;
        }

        if matches!(ch, '{' | '}' | '[' | ']') {
            append_chars(&mut job, &chars[i..=i], brace.clone());
            i += 1;
            continue;
        }

        if in_inline_math {
            let start = i;
            while i < chars.len() && chars[i] != '$' {
                i += 1;
            }
            append_chars(&mut job, &chars[start..i], math.clone());
            continue;
        }

        let start = i;
        while i < chars.len()
            && chars[i] != '%'
            && chars[i] != '\\'
            && chars[i] != '$'
            && !matches!(chars[i], '{' | '}' | '[' | ']')
        {
            i += 1;
        }
        append_chars(&mut job, &chars[start..i], default.clone());
    }

    apply_diagnostic_underlines(&mut job, text, diagnostics);
    job
}

fn append_chars(job: &mut LayoutJob, chars: &[char], format: TextFormat) {
    let text: String = chars.iter().collect();
    job.append(&text, 0.0, format);
}

fn apply_diagnostic_underlines(job: &mut LayoutJob, text: &str, diagnostics: &[Diagnostic]) {
    let byte_offsets = char_to_byte_offsets(text);

    for diagnostic in diagnostics {
        let (Some(start_char), Some(end_char)) = (diagnostic.start_char, diagnostic.end_char) else {
            continue;
        };
        let Some(&start_byte) = byte_offsets.get(start_char) else {
            continue;
        };
        let Some(&end_byte) = byte_offsets.get(end_char) else {
            continue;
        };
        if start_byte >= end_byte {
            continue;
        }

        let underline = match diagnostic.severity {
            Severity::Info => continue,
            Severity::Warning => egui::Stroke::new(1.5, Color32::from_rgb(240, 190, 90)),
            Severity::Error => egui::Stroke::new(1.5, Color32::from_rgb(230, 90, 90)),
        };

        let mut new_sections = Vec::with_capacity(job.sections.len() + 2);
        for section in &job.sections {
            let overlap_start = start_byte.max(section.byte_range.start);
            let overlap_end = end_byte.min(section.byte_range.end);
            if overlap_start >= overlap_end {
                new_sections.push(section.clone());
                continue;
            }

            if section.byte_range.start < overlap_start {
                let mut before = section.clone();
                before.byte_range = section.byte_range.start..overlap_start;
                new_sections.push(before);
            }

            let mut middle = section.clone();
            middle.byte_range = overlap_start..overlap_end;
            middle.format.underline = underline;
            new_sections.push(middle);

            if overlap_end < section.byte_range.end {
                let mut after = section.clone();
                after.byte_range = overlap_end..section.byte_range.end;
                new_sections.push(after);
            }
        }
        job.sections = merge_adjacent_sections(new_sections);
    }
}

fn merge_adjacent_sections(
    sections: Vec<egui::text::LayoutSection>,
) -> Vec<egui::text::LayoutSection> {
    let mut merged: Vec<egui::text::LayoutSection> = Vec::with_capacity(sections.len());
    for section in sections {
        if let Some(last) = merged.last_mut() {
            if last.byte_range.end == section.byte_range.start
                && last.leading_space == section.leading_space
                && last.format == section.format
            {
                last.byte_range.end = section.byte_range.end;
                continue;
            }
        }
        merged.push(section);
    }
    merged
}

fn char_to_byte_offsets(text: &str) -> Vec<usize> {
    let mut offsets: Vec<usize> = text.char_indices().map(|(idx, _)| idx).collect();
    offsets.push(text.len());
    offsets
}

fn configure_fonts(ctx: &egui::Context) {
    let mut fonts = FontDefinitions::default();
    if let Some(font_data) = load_japanese_font() {
        let name = "jp-ui".to_owned();
        fonts
            .font_data
            .insert(name.clone(), egui::FontData::from_owned(font_data).into());
        fonts
            .families
            .entry(FontFamily::Proportional)
            .or_default()
            .insert(0, name.clone());
        fonts
            .families
            .entry(FontFamily::Monospace)
            .or_default()
            .insert(0, name);
    }
    ctx.set_fonts(fonts);
}

fn load_japanese_font() -> Option<Vec<u8>> {
    preferred_japanese_font_paths()
        .into_iter()
        .find_map(|path| fs::read(path).ok())
}

fn preferred_japanese_font_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();
    if let Some(path) = fontconfig_japanese_font_path() {
        paths.push(path);
    }
    paths.extend(known_font_paths());
    paths
}

fn fontconfig_japanese_font_path() -> Option<PathBuf> {
    let fc_match = resolve_command_path("fc-match")?;
    let output = configure_command(Command::new(fc_match))
        .args(["-f", "%{file}\n", ":lang=ja"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_owned();
    if path.is_empty() {
        None
    } else {
        Some(PathBuf::from(path))
    }
}

fn resolve_command_path(command: &str) -> Option<String> {
    std::env::var_os("PATH")
        .into_iter()
        .flat_map(|paths| std::env::split_paths(&paths).collect::<Vec<_>>())
        .flat_map(|dir| path_candidates_for_command(&dir, command))
        .find(|path| path.exists())
        .or_else(|| known_command_paths(command).into_iter().find(|path| path.exists()))
        .map(|path| path.to_string_lossy().into_owned())
}

fn path_candidates_for_command(dir: &Path, command: &str) -> Vec<PathBuf> {
    let mut candidates = vec![dir.join(command)];
    if cfg!(windows) && !command.contains('.') {
        candidates.push(dir.join(format!("{command}.exe")));
    }
    candidates
}

fn known_command_paths(command: &str) -> Vec<PathBuf> {
    let mut candidates = match command {
        "latexmk" => find_texlive_command_candidates(command),
        "pdftoppm" => find_poppler_command_candidates(command),
        "tectonic" => find_tectonic_command_candidates(command),
        "hunspell" => bundled_hunspell_command_paths(),
        _ => Vec::new(),
    };

    candidates.extend(static_fallback_command_paths(command));
    candidates
}

fn bundled_hunspell_command_paths() -> Vec<PathBuf> {
    app_search_roots()
        .into_iter()
        .flat_map(|root| {
            [
                root.join("hunspell.exe"),
                root.join("dist-win64").join("hunspell.exe"),
                root.join("hunspell").join("hunspell.exe"),
                root.join("hunspell").join("dist-win64").join("hunspell.exe"),
                root.join("tools").join("hunspell").join("hunspell.exe"),
                root.join("tools").join("hunspell").join("dist-win64").join("hunspell.exe"),
            ]
        })
        .collect()
}

fn static_fallback_command_paths(command: &str) -> Vec<PathBuf> {
    match command {
        "latexmk" => vec![
            PathBuf::from(r"C:\texlive\2025\bin\windows\latexmk.exe"),
            PathBuf::from(r"C:\texlive\2024\bin\windows\latexmk.exe"),
            PathBuf::from(r"C:\texlive\2023\bin\windows\latexmk.exe"),
            PathBuf::from("/usr/bin/latexmk"),
            PathBuf::from("/usr/local/bin/latexmk"),
            PathBuf::from("/Library/TeX/texbin/latexmk"),
        ],
        "pdftoppm" => vec![
            PathBuf::from(r"C:\Program Files\poppler\Library\bin\pdftoppm.exe"),
            PathBuf::from(r"C:\Program Files (x86)\poppler\Library\bin\pdftoppm.exe"),
            PathBuf::from("/usr/bin/pdftoppm"),
            PathBuf::from("/usr/local/bin/pdftoppm"),
            PathBuf::from("/opt/homebrew/bin/pdftoppm"),
        ],
        "tectonic" => vec![
            PathBuf::from(r"C:\Program Files\Tectonic\tectonic.exe"),
            PathBuf::from(r"C:\Program Files (x86)\Tectonic\tectonic.exe"),
            PathBuf::from("/usr/bin/tectonic"),
            PathBuf::from("/usr/local/bin/tectonic"),
            PathBuf::from("/opt/homebrew/bin/tectonic"),
        ],
        "git" => vec![
            PathBuf::from(r"C:\Program Files\Git\cmd\git.exe"),
            PathBuf::from(r"C:\Program Files\Git\bin\git.exe"),
            PathBuf::from("/usr/bin/git"),
            PathBuf::from("/usr/local/bin/git"),
            PathBuf::from("/opt/homebrew/bin/git"),
        ],
        "gh" => vec![
            PathBuf::from(r"C:\Program Files\GitHub CLI\gh.exe"),
            PathBuf::from(r"C:\Program Files (x86)\GitHub CLI\gh.exe"),
            PathBuf::from("/usr/bin/gh"),
            PathBuf::from("/usr/local/bin/gh"),
            PathBuf::from("/opt/homebrew/bin/gh"),
        ],
        "lualatex" => vec![
            PathBuf::from(r"C:\texlive\2025\bin\windows\lualatex.exe"),
            PathBuf::from(r"C:\texlive\2024\bin\windows\lualatex.exe"),
            PathBuf::from(r"C:\texlive\2023\bin\windows\lualatex.exe"),
            PathBuf::from("/usr/bin/lualatex"),
            PathBuf::from("/usr/local/bin/lualatex"),
            PathBuf::from("/Library/TeX/texbin/lualatex"),
        ],
        "hunspell" => vec![
            PathBuf::from(r"C:\Program Files\Hunspell\bin\hunspell.exe"),
            PathBuf::from(r"C:\Program Files\Hunspell\hunspell.exe"),
            PathBuf::from("/usr/bin/hunspell"),
            PathBuf::from("/usr/local/bin/hunspell"),
            PathBuf::from("/opt/homebrew/bin/hunspell"),
        ],
        "texlab" => vec![
            PathBuf::from(r"C:\Program Files\texlab\texlab.exe"),
            PathBuf::from(r"C:\Program Files (x86)\texlab\texlab.exe"),
            PathBuf::from("/usr/bin/texlab"),
            PathBuf::from("/usr/local/bin/texlab"),
            PathBuf::from("/opt/homebrew/bin/texlab"),
        ],
        "fc-match" => vec![
            PathBuf::from("/usr/bin/fc-match"),
            PathBuf::from("/usr/local/bin/fc-match"),
            PathBuf::from("/opt/homebrew/bin/fc-match"),
        ],
        _ => Vec::new(),
    }
}

fn find_texlive_command_candidates(command: &str) -> Vec<PathBuf> {
    let mut roots = vec![PathBuf::from(r"C:\texlive")];
    if let Some(root) = std::env::var_os("TEXLIVE_HOME") {
        roots.push(PathBuf::from(root));
    }

    let mut candidates = Vec::new();
    for root in roots {
        let Ok(entries) = fs::read_dir(&root) else {
            continue;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            if !path.is_dir() {
                continue;
            }

            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if !name.chars().all(|ch| ch.is_ascii_digit()) {
                continue;
            }

            candidates.push(path.join("bin").join("windows").join(format!("{command}.exe")));
            candidates.push(path.join("bin").join("win32").join(format!("{command}.exe")));
        }
    }

    candidates.sort();
    candidates.reverse();
    candidates
}

fn find_poppler_command_candidates(command: &str) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for root in windows_program_roots() {
        candidates.extend(find_matching_subdir_commands(
            &root,
            "poppler",
            &[&["Library", "bin"], &["bin"]],
            command,
        ));
    }
    candidates
}

fn find_tectonic_command_candidates(command: &str) -> Vec<PathBuf> {
    let mut candidates = Vec::new();
    for root in windows_program_roots() {
        candidates.push(root.join("Tectonic").join(format!("{command}.exe")));
        candidates.extend(find_matching_subdir_commands(
            &root,
            "Tectonic",
            &[&[]],
            command,
        ));
    }
    candidates
}

fn windows_program_roots() -> Vec<PathBuf> {
    let mut roots = Vec::new();
    for key in ["ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"] {
        if let Some(value) = std::env::var_os(key) {
            roots.push(PathBuf::from(value));
        }
    }
    roots.push(PathBuf::from(r"C:\Program Files"));
    roots.push(PathBuf::from(r"C:\Program Files (x86)"));
    roots.push(PathBuf::from(r"C:\Users"));
    roots
}

fn find_matching_subdir_commands(
    root: &Path,
    prefix: &str,
    suffix_sets: &[&[&str]],
    command: &str,
) -> Vec<PathBuf> {
    let Ok(entries) = fs::read_dir(root) else {
        return Vec::new();
    };

    let mut candidates = Vec::new();
    for entry in entries.flatten() {
        let base = entry.path();
        if !base.is_dir() {
            continue;
        }

        let Some(name) = base.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !name.to_ascii_lowercase().starts_with(&prefix.to_ascii_lowercase()) {
            continue;
        }

        for suffix in suffix_sets {
            let mut candidate = base.clone();
            for segment in *suffix {
                candidate.push(segment);
            }
            candidate.push(format!("{command}.exe"));
            candidates.push(candidate);
        }
    }
    candidates
}

fn known_font_paths() -> Vec<PathBuf> {
    let mut paths = vec![
        PathBuf::from("/usr/share/fonts/truetype/droid/DroidSansFallbackFull.ttf"),
        PathBuf::from("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"),
        PathBuf::from("/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.otf"),
        PathBuf::from("/usr/share/fonts/opentype/noto/NotoSerifCJK-Regular.ttc"),
        PathBuf::from("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc"),
        PathBuf::from("/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.otf"),
        PathBuf::from("/usr/share/fonts/truetype/ipafont-gothic/ipag.ttf"),
        PathBuf::from("/usr/share/fonts/OTF/ipag.ttf"),
        PathBuf::from("/usr/share/fonts/opentype/ipafont-gothic/ipagp.ttf"),
        PathBuf::from("/usr/share/fonts/opentype/ipafont-mincho/ipamp.ttf"),
        PathBuf::from("/System/Library/Fonts/Hiragino Sans GB.ttc"),
        PathBuf::from("/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc"),
        PathBuf::from("/Library/Fonts/Arial Unicode.ttf"),
    ];

    if let Some(home) = std::env::var_os("HOME") {
        let home = PathBuf::from(home);
        paths.push(home.join(".fonts").join("NotoSansCJK-Regular.ttc"));
        paths.push(home.join(".local").join("share").join("fonts").join("NotoSansCJK-Regular.ttc"));
        paths.push(home.join(".local").join("share").join("fonts").join("NotoSansCJK-Regular.otf"));
    }

    paths
}
