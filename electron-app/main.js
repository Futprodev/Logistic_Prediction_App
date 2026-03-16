const { app, BrowserWindow, shell, ipcMain } = require("electron");
const path       = require("path");
const scheduler  = require("../api/services/scheduler");
const isDev      = !app.isPackaged;

function createWindow() {
  const win = new BrowserWindow({
    width: 1400,
    height: 900,
    minWidth: 1100,
    minHeight: 700,
    titleBarStyle: "hiddenInset",
    backgroundColor: "#0a0e1a",
    webPreferences: {
      nodeIntegration: false,
      contextIsolation: true,
      preload: path.join(__dirname, "preload.js"),
    },
    icon: path.join(__dirname, "assets/icon.png"),
  });

  if (isDev) {
    win.loadURL("http://localhost:3000");
    win.webContents.openDevTools();
  } else {
    win.loadFile(path.join(__dirname, "build/index.html"));
  }

  win.webContents.setWindowOpenHandler(({ url }) => {
    shell.openExternal(url);
    return { action: "deny" };
  });
}

// ── IPC handlers for scheduler status ────────────────────────────────────────
ipcMain.handle("scheduler:status", () => scheduler.getStatus());
ipcMain.handle("scheduler:run-now", (_, source) => {
  return new Promise(resolve => {
    const { spawn } = require("child_process");
    const path      = require("path");
    const root      = path.join(__dirname, "..");
    const proc      = spawn("python", [path.join(root, "run_pipeline.py"), "--source", source], {
      cwd: root, windowsHide: true,
    });
    proc.on("close", code => resolve({ success: code === 0 }));
    proc.on("error", err => resolve({ success: false, error: err.message }));
  });
});

app.whenReady().then(() => {
  createWindow();
  // Start scheduler after window is ready
  // In dev, skip scheduler to avoid running pipeline constantly
  if (!isDev) {
    scheduler.start();
  } else {
    console.log("[Main] Dev mode — scheduler disabled. Run pipeline manually.");
  }
});

app.on("window-all-closed", () => {
  scheduler.stop();
  if (process.platform !== "darwin") app.quit();
});

app.on("activate", () => {
  if (BrowserWindow.getAllWindows().length === 0) createWindow();
});