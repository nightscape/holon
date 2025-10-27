import React from 'react';
import { isBrowserMode } from '../lib/env';
import { fakeBackend } from '../lib/fakeBackend';
import { useBlockStore } from '../store/blockStore';
import { IconRefresh, IconTrash, IconDatabase } from '@tabler/icons-react';

export const DevModeToolbar: React.FC = () => {
  const { loadBlocks } = useBlockStore();

  if (!isBrowserMode()) {
    return null;
  }

  const handleSeedData = async () => {
    if (confirm('This will replace all current data with sample data. Continue?')) {
      fakeBackend.seedSample();
      await loadBlocks();
    }
  };

  const handleClearData = async () => {
    if (confirm('This will delete all data. Continue?')) {
      fakeBackend.clearAll();
      await loadBlocks();
    }
  };

  const handleRefresh = async () => {
    await loadBlocks();
  };

  return (
    <div className="fixed bottom-4 right-4 bg-amber-100 dark:bg-amber-900 border-2 border-amber-500 rounded-lg shadow-lg p-3 z-50">
      <div className="flex items-center gap-2 mb-2">
        <div className="flex items-center gap-1 text-amber-800 dark:text-amber-200 font-semibold text-sm">
          <IconDatabase size={16} />
          <span>Browser Dev Mode</span>
        </div>
      </div>

      <div className="text-xs text-amber-700 dark:text-amber-300 mb-2">
        Running with localStorage mock data
      </div>

      <div className="flex gap-2">
        <button
          onClick={handleRefresh}
          className="flex items-center gap-1 px-2 py-1 text-xs bg-blue-500 text-white rounded hover:bg-blue-600 transition-colors"
          title="Refresh data"
        >
          <IconRefresh size={14} />
          Refresh
        </button>

        <button
          onClick={handleSeedData}
          className="flex items-center gap-1 px-2 py-1 text-xs bg-green-500 text-white rounded hover:bg-green-600 transition-colors"
          title="Load sample data"
        >
          <IconDatabase size={14} />
          Seed
        </button>

        <button
          onClick={handleClearData}
          className="flex items-center gap-1 px-2 py-1 text-xs bg-red-500 text-white rounded hover:bg-red-600 transition-colors"
          title="Clear all data"
        >
          <IconTrash size={14} />
          Clear
        </button>
      </div>

      <div className="mt-2 text-xs text-amber-600 dark:text-amber-400">
        💡 Data persists in localStorage
      </div>
    </div>
  );
};
