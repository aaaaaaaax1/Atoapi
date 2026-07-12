import {
  LogicalPosition,
  LogicalSize,
  currentMonitor,
  getCurrentWindow,
} from "@tauri-apps/api/window";

const DEFAULT_WIDTH = 1100;
const DEFAULT_HEIGHT = 660;
const WORK_AREA_MARGIN = 48;
const MIN_WIDTH = 900;
const MIN_HEIGHT = 520;

function isTauriRuntime() {
  return typeof window !== "undefined" && "__TAURI_INTERNALS__" in window;
}

export async function fitInitialWindowToCurrentMonitor() {
  if (!isTauriRuntime()) {
    return;
  }

  try {
    const appWindow = getCurrentWindow();
    const monitor = await currentMonitor();

    if (!monitor) {
      return;
    }

    const workAreaSize = monitor.workArea.size.toLogical(monitor.scaleFactor);
    const workAreaPosition = monitor.workArea.position.toLogical(monitor.scaleFactor);
    const maxWidth = Math.max(MIN_WIDTH, Math.floor(workAreaSize.width - WORK_AREA_MARGIN));
    const maxHeight = Math.max(MIN_HEIGHT, Math.floor(workAreaSize.height - WORK_AREA_MARGIN));
    const targetWidth = Math.min(DEFAULT_WIDTH, maxWidth);
    const targetHeight = Math.min(DEFAULT_HEIGHT, maxHeight);

    await appWindow.setSizeConstraints({
      minWidth: Math.min(MIN_WIDTH, targetWidth),
      minHeight: Math.min(MIN_HEIGHT, targetHeight),
    });
    await appWindow.setSize(new LogicalSize(targetWidth, targetHeight));

    const outerSize = (await appWindow.outerSize()).toLogical(monitor.scaleFactor);
    const x = Math.round(
      workAreaPosition.x + Math.max(0, (workAreaSize.width - outerSize.width) / 2),
    );
    const y = Math.round(
      workAreaPosition.y + Math.max(0, (workAreaSize.height - outerSize.height) / 2),
    );

    await appWindow.setPosition(new LogicalPosition(x, y));
  } catch (error) {
    console.warn("Unable to fit the startup window to the current monitor.", error);
  }
}
