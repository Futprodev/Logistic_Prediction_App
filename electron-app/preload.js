const { contextBridge } = require("electron");

// Expose safe APIs to renderer if needed in future
contextBridge.exposeInMainWorld("electron", {
  platform: process.platform,
});