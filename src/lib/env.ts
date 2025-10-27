/**
 * Environment detection utilities for Tauri app
 */

/**
 * Check if running inside Tauri (desktop app)
 */
export function isTauri(): boolean {
  return typeof window !== 'undefined' && '__TAURI__' in window;
}

/**
 * Check if running in browser development mode
 */
export function isBrowserMode(): boolean {
  return !isTauri();
}

/**
 * Get development mode from localStorage or default to true in browser
 */
export function isDevelopmentMode(): boolean {
  if (isTauri()) {
    return false;
  }

  try {
    const stored = localStorage.getItem('dev-mode-enabled');
    return stored !== 'false'; // Default to true
  } catch {
    return true;
  }
}

/**
 * Enable or disable development mode (browser only)
 */
export function setDevelopmentMode(enabled: boolean): void {
  if (isBrowserMode()) {
    try {
      localStorage.setItem('dev-mode-enabled', String(enabled));
    } catch (error) {
      console.warn('Failed to set development mode:', error);
    }
  }
}
