; Inno Setup Script for Graph-Loom
; This script creates a Windows installer for the Graph-Loom Rust application

#define MyAppName "Graph-Loom"
#define MyAppVersion "1.9.2"
#define MyAppPublisher "jackpots28"
#define MyAppURL "https://github.com/jackpots28"
#define MyAppExeName "Graph-Loom.exe"

[Setup]
; NOTE: The value of AppId uniquely identifies this application.
; Do not use the same AppId value in installers for other applications.
AppId={{39092294-7058-4BE5-AC66-EDD64B8618A9}}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}
DefaultDirName={autopf}\{#MyAppName}
DefaultGroupName={#MyAppName}
AllowNoIcons=yes
; Uncomment the following line to run in non administrative install mode (install for current user only.)
;PrivilegesRequired=lowest
PrivilegesRequiredOverridesAllowed=dialog
OutputDir=.\output
OutputBaseFilename=graph-loom-setup
SetupIconFile=.\icon\graph-loom.ico
Compression=lzma
SolidCompression=yes
WizardStyle=modern
UninstallDisplayIcon={app}\{#MyAppExeName}

[Languages]
Name: "english"; MessagesFile: "compiler:Default.isl"

[Tasks]
Name: "desktopicon"; Description: "{cm:CreateDesktopIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked
Name: "quicklaunchicon"; Description: "{cm:CreateQuickLaunchIcon}"; GroupDescription: "{cm:AdditionalIcons}"; Flags: unchecked; OnlyBelowVersion: 6.1; Check: not IsAdminInstallMode

[Files]
; Main executable - adjust the source path to match your Rust build output
Source: "..\target\release\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
; Include any additional DLLs or dependencies your Rust app needs
; Source: "..\target\release\*.dll"; DestDir: "{app}"; Flags: ignoreversion
; Include assets directory if needed
; Source: "..\assets\*"; DestDir: "{app}\assets"; Flags: ignoreversion recursesubdirs createallsubdirs
; Include any configuration files
; Source: "..\config\*"; DestDir: "{app}\config"; Flags: ignoreversion recursesubdirs createallsubdirs
; Include README and LICENSE files
Source: "..\README.md"; DestDir: "{app}"; Flags: ignoreversion; DestName: "README.txt"
Source: "..\LICENSE"; DestDir: "{app}"; Flags: ignoreversion; DestName: "LICENSE.txt"

[Icons]
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; IconFilename: "{app}\{#MyAppExeName}"
Name: "{group}\{cm:UninstallProgram,{#MyAppName}}"; Filename: "{uninstallexe}"
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: desktopicon; IconFilename: "{app}\{#MyAppExeName}"
Name: "{userappdata}\Microsoft\Internet Explorer\Quick Launch\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; Tasks: quicklaunchicon; IconFilename: "{app}\{#MyAppExeName}"

[Run]
Filename: "{app}\{#MyAppExeName}"; Description: "{cm:LaunchProgram,{#StringChange(MyAppName, '&', '&&')}}"; Flags: nowait postinstall skipifsilent

[Code]
function GetUninstallString(): String;
var
  sUnInstPath: String;
  sUnInstallString: String;
begin
  sUnInstPath := ExpandConstant('Software\Microsoft\Windows\CurrentVersion\Uninstall\{#emit SetupSetting("AppId")}_is1');
  sUnInstallString := '';
  if not RegQueryStringValue(HKLM, sUnInstPath, 'UninstallString', sUnInstallString) then
    RegQueryStringValue(HKCU, sUnInstPath, 'UninstallString', sUnInstallString);
  Result := sUnInstallString;
end;

function IsUpgrade(): Boolean;
begin
  Result := (GetUninstallString() <> '');
end;

function InitializeSetup(): Boolean;
var
  V: Integer;
  iResultCode: Integer;
  sUnInstallString: String;
begin
  Result := True;
  if IsUpgrade() then
  begin
    V := MsgBox(ExpandConstant('An existing installation of {#MyAppName} was detected. Do you want to uninstall it before continuing?'), mbConfirmation, MB_YESNO);
    if V = IDYES then
    begin
      sUnInstallString := GetUninstallString();
      sUnInstallString := RemoveQuotes(sUnInstallString);
      Exec(sUnInstallString, '/SILENT /VERYSILENT /NORESTART /SUPPRESSMSGBOXES', '', SW_SHOW, ewWaitUntilTerminated, iResultCode);
      Result := True; // Continue with installation
    end;
  end;
end;
