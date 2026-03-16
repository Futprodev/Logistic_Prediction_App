const { contextBridge, ipcRenderer } = require("electron");

contextBridge.exposeInMainWorld("electron", {
  platform: process.platform,
  scheduler: {
    getStatus: ()       => ipcRenderer.invoke("scheduler:status"),
    runNow:    (source) => ipcRenderer.invoke("scheduler:run-now", source),
  },
});