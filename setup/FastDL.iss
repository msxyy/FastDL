; ============================================================
; FastDL Inno Setup Script
; 打包为单个安装程序，全程中文界面
; ============================================================

#define MyAppName "FastDL"
#define MyAppVersion "0.5.0"
#define MyAppPublisher "FastDL"
#define MyAppURL "https://github.com/yourname/fastdl"
#define MyAppExeName "fastdl.exe"
#define MyLauncherExeName "fastdl-launcher.exe"

[Setup]
; 应用基本信息
AppId={{A1B2C3D4-E5F6-7890-ABCD-EF1234567890}
AppName={#MyAppName}
AppVersion={#MyAppVersion}
AppPublisher={#MyAppPublisher}
AppPublisherURL={#MyAppURL}
AppSupportURL={#MyAppURL}
AppUpdatesURL={#MyAppURL}

; 关键：强制最低权限，注册表写 HKCU，路径走 localappdata
PrivilegesRequired=lowest

; 默认安装路径：用户目录下的 FastDL 文件夹，与 install.ps1 保持一致
DefaultDirName={localappdata}\FastDL
DisableProgramGroupPage=yes
DisableDirPage=no
DefaultGroupName={#MyAppName}

; 安装程序输出
OutputDir=..\installer_output
OutputBaseFilename=FastDL-Setup-{#MyAppVersion}
Compression=lzma2
SolidCompression=yes
WizardStyle=modern
UninstallDisplayName={#MyAppName}

; 安装程序图标（用编译器默认的，可选自定义）
; SetupIconFile=..\extension\icon128.ico

[Languages]
Name: "chinesesimplified"; MessagesFile: "compiler:Languages\ChineseSimplified.isl"

[Tasks]
; 桌面快捷方式
Name: "desktopicon"; Description: "创建桌面快捷方式"; GroupDescription: "附加任务："; Flags: unchecked

[Files]
; 主程序文件
Source: "..\target\release\{#MyAppExeName}"; DestDir: "{app}"; Flags: ignoreversion
Source: "..\target\release\{#MyLauncherExeName}"; DestDir: "{app}"; Flags: ignoreversion

; 扩展文件夹（整个目录打包）
Source: "..\extension\*"; DestDir: "{app}\extension"; Flags: ignoreversion recursesubdirs createallsubdirs

; install.ps1 一起打包，方便用户配扩展 ID
Source: "..\installer\install.ps1"; DestDir: "{app}"; Flags: ignoreversion

; 说明文件（如果存在）
Source: "..\README.txt"; DestDir: "{app}"; Flags: ignoreversion skipifsourcedoesntexist

[Icons]
; 开始菜单
Name: "{group}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; WorkingDir: "{app}"
Name: "{group}\卸载 {#MyAppName}"; Filename: "{uninstallexe}"

; 桌面快捷方式（仅当勾选）
Name: "{autodesktop}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; WorkingDir: "{app}"; Tasks: desktopicon

; 开机自启动（当前用户）
Name: "{userstartup}\{#MyAppName}"; Filename: "{app}\{#MyAppExeName}"; WorkingDir: "{app}"

[Registry]
; Native Messaging Host (Chrome) — 指向安装时用 [Code] 生成的 JSON
Root: HKCU; Subkey: "Software\Google\Chrome\NativeMessagingHosts\com.fastdl.launcher"; ValueType: string; ValueName: ""; ValueData: "{app}\com.fastdl.launcher.json"; Flags: uninsdeletekey

; Native Messaging Host (Edge)
Root: HKCU; Subkey: "Software\Microsoft\Edge\NativeMessagingHosts\com.fastdl.launcher"; ValueType: string; ValueName: ""; ValueData: "{app}\com.fastdl.launcher.json"; Flags: uninsdeletekey

[Run]
; 安装完成后可选启动 FastDL
Filename: "{app}\{#MyAppExeName}"; Description: "立即运行 {#MyAppName}"; Flags: nowait postinstall skipifsilent

; 安装完成后提示用户配置扩展 ID（打开一个说明窗）
Filename: "{cmd}"; Parameters: "/K echo. & echo ======================================== & echo 下一步：配置浏览器扩展 & echo ======================================== & echo. & echo 1. 打开 chrome://extensions 并加载已解压的扩展： & echo    {app}\extension & echo. & echo 2. 抄下扩展 ID（32 个小写字母） & echo. & echo 3. 运行下面的脚本并粘贴 ID： & echo    {app}\install.ps1 & echo. & echo 4. 完成后即可关闭本窗口。 & echo. & pause"; Description: "配置浏览器扩展 ID"; Flags: postinstall nowait skipifsilent

[UninstallRun]
; 卸载前先结束 FastDL 进程
Filename: "taskkill"; Parameters: "/F /IM {#MyAppExeName}"; Flags: runhidden
Filename: "taskkill"; Parameters: "/F /IM {#MyLauncherExeName}"; Flags: runhidden

[UninstallDelete]
; 清理运行时产生的文件
Type: filesandordirs; Name: "{app}\logs"
Type: files; Name: "{app}\tasks.json"
Type: files; Name: "{app}\com.fastdl.launcher.json"