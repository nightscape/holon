import { afterEach, beforeAll } from 'vitest';
import { cleanup } from '@testing-library/react';
import '@testing-library/jest-dom/vitest';
import { mockIPC, clearMocks } from '@tauri-apps/api/mocks';
import { fakeBackend } from '../lib/fakeBackend';

// Set up Tauri's mockIPC to delegate to our fake backend
// This ensures tests use the same backend as browser dev mode
beforeAll(() => {
  // Disable localStorage persistence in tests to avoid cross-test pollution
  fakeBackend.setPersistence(false);

  // Delegate all IPC calls to the fake backend
  mockIPC((cmd, args) => {
    return fakeBackend.invoke(cmd, args);
  });
});

afterEach(() => {
  cleanup();
  // Clear the fake backend state between tests
  fakeBackend.clearAll();
  clearMocks();
});
