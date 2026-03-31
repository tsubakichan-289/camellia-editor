#define AppName "camellia-editor"
#define AppVersion "0.1.0"
#define AppPublisher "tsubakichan-289"
#define AppExeName "camellia-editor.exe"
#define AppDir "camellia-editor"

[Setup]
AppId={{6F4D0F17-2B9B-45E8-B6F5-7A5E8D0E7C41}
AppName={#AppName}
AppVersion={#AppVersion}
AppPublisher={#AppPublisher}
DefaultDirName={autopf}\{#AppName}
DefaultGroupName={#AppName}
DisableProgramGroupPage=yes
OutputDir=installer-dist
OutputBaseFilename=camellia-editor-setup
Compression=lzma
SolidCompression=yes
WizardStyle=modern
UninstallDisplayIcon={app}\{#AppExeName}
ArchitecturesInstallIn64BitMode=x64compatible

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; Flags: unchecked
Name: "contextmenu"; Description: "Add 'Open with camellia-editor' to folder context menus"; Flags: unchecked

[Files]
Source: "{#AppDir}\*"; DestDir: "{app}"; Flags: ignoreversion recursesubdirs createallsubdirs

[Icons]
Name: "{group}\{#AppName}"; Filename: "{app}\{#AppExeName}"
Name: "{autodesktop}\{#AppName}"; Filename: "{app}\{#AppExeName}"; Tasks: desktopicon

[Registry]
Root: HKCR; Subkey: "Directory\shell\camellia-editor"; ValueType: string; ValueName: ""; ValueData: "Open with camellia-editor"; Tasks: contextmenu; Flags: uninsdeletekey
Root: HKCR; Subkey: "Directory\shell\camellia-editor"; ValueType: string; ValueName: "Icon"; ValueData: "{app}\{#AppExeName}"; Tasks: contextmenu
Root: HKCR; Subkey: "Directory\shell\camellia-editor\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}"" ""%1"""; Tasks: contextmenu; Flags: uninsdeletekey
Root: HKCR; Subkey: "Directory\Background\shell\camellia-editor"; ValueType: string; ValueName: ""; ValueData: "Open with camellia-editor"; Tasks: contextmenu; Flags: uninsdeletekey
Root: HKCR; Subkey: "Directory\Background\shell\camellia-editor"; ValueType: string; ValueName: "Icon"; ValueData: "{app}\{#AppExeName}"; Tasks: contextmenu
Root: HKCR; Subkey: "Directory\Background\shell\camellia-editor\command"; ValueType: string; ValueName: ""; ValueData: """{app}\{#AppExeName}"" ""%V"""; Tasks: contextmenu; Flags: uninsdeletekey

[Run]
Filename: "{app}\{#AppExeName}"; Description: "Launch camellia-editor"; Flags: nowait postinstall skipifsilent
