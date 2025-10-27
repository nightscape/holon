import React, { useState } from 'react';
import {
  IconCalendar,
  IconCards,
  IconFile,
  IconNetwork,
  IconChevronDown,
  IconChevronRight,
  IconSparkles,
  IconHistory
} from '@tabler/icons-react';

interface LeftSidebarProps {
  isOpen: boolean;
}

export const LeftSidebar: React.FC<LeftSidebarProps> = ({ isOpen }) => {
  const [navigationsOpen, setNavigationsOpen] = useState(true);
  const [favoritesOpen, setFavoritesOpen] = useState(true);
  const [recentOpen, setRecentOpen] = useState(true);

  if (!isOpen) return null;

  return (
    <aside className="left-sidebar fixed left-0 top-12 bottom-0 w-60 bg-gray-50 dark:bg-gray-800 border-r border-gray-200 dark:border-gray-700 overflow-y-auto z-40">
      <div className="p-4">
        {/* Graph Selector */}
        <div className="mb-6">
          <button className="flex items-center w-full px-3 py-2 rounded hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors">
            <IconSparkles size={16} className="text-gray-600 dark:text-gray-400 mr-2" />
            <span className="font-semibold text-sm text-gray-800 dark:text-gray-200 flex-1 text-left">
              Demo
            </span>
            <IconChevronDown size={16} className="text-gray-400" />
          </button>
        </div>

        {/* Navigations Section */}
        <div className="mb-6">
          <button
            onClick={() => setNavigationsOpen(!navigationsOpen)}
            className="flex items-center w-full px-3 py-1 mb-2 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-300"
          >
            {navigationsOpen ? (
              <IconChevronDown size={14} className="mr-1" />
            ) : (
              <IconChevronRight size={14} className="mr-1" />
            )}
            Navigations
          </button>

          {navigationsOpen && (
            <nav className="space-y-1">
              <a
                href="#/journals"
                className="flex items-center px-3 py-2 text-sm rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 transition-colors"
              >
                <IconCalendar size={18} className="mr-3 text-gray-500 dark:text-gray-400" />
                Journals
              </a>

              <a
                href="#/flashcards"
                className="flex items-center px-3 py-2 text-sm rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 transition-colors"
              >
                <IconCards size={18} className="mr-3 text-gray-500 dark:text-gray-400" />
                Flashcards
              </a>

              <a
                href="#/all-pages"
                className="flex items-center px-3 py-2 text-sm rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 transition-colors"
              >
                <IconFile size={18} className="mr-3 text-gray-500 dark:text-gray-400" />
                Pages
              </a>

              <a
                href="#/graph"
                className="flex items-center px-3 py-2 text-sm rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 transition-colors"
              >
                <IconNetwork size={18} className="mr-3 text-gray-500 dark:text-gray-400" />
                Graph view
              </a>
            </nav>
          )}
        </div>

        {/* Favorites Section */}
        <div className="mb-6">
          <button
            onClick={() => setFavoritesOpen(!favoritesOpen)}
            className="flex items-center w-full px-3 py-1 mb-2 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-300"
          >
            {favoritesOpen ? (
              <IconChevronDown size={14} className="mr-1" />
            ) : (
              <IconChevronRight size={14} className="mr-1" />
            )}
            Favorites
          </button>

          {favoritesOpen && (
            <div className="px-3 py-2 text-sm text-gray-500 dark:text-gray-400 italic">
              No favorites yet
            </div>
          )}
        </div>

        {/* Recent Section */}
        <div className="mb-6">
          <button
            onClick={() => setRecentOpen(!recentOpen)}
            className="flex items-center w-full px-3 py-1 mb-2 text-xs font-semibold text-gray-500 dark:text-gray-400 uppercase tracking-wider hover:text-gray-700 dark:hover:text-gray-300"
          >
            {recentOpen ? (
              <IconChevronDown size={14} className="mr-1" />
            ) : (
              <IconChevronRight size={14} className="mr-1" />
            )}
            Recent
          </button>

          {recentOpen && (
            <nav className="space-y-1">
              <a
                href="#/recent-page"
                className="flex items-center px-3 py-2 text-sm rounded hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-700 dark:text-gray-300 transition-colors"
              >
                <IconHistory size={18} className="mr-3 text-gray-500 dark:text-gray-400" />
                <span className="truncate">Recent Page Example</span>
              </a>
            </nav>
          )}
        </div>
      </div>
    </aside>
  );
};
