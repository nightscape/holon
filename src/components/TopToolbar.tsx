import React from 'react';
import {
  IconMenu2,
  IconSearch,
  IconHome,
  IconDots,
  IconLayoutSidebarRightCollapse,
  IconLayoutSidebarRightExpand
} from '@tabler/icons-react';

interface TopToolbarProps {
  onToggleLeftSidebar: () => void;
  onToggleRightSidebar: () => void;
  leftSidebarOpen: boolean;
  rightSidebarOpen: boolean;
}

export const TopToolbar: React.FC<TopToolbarProps> = ({
  onToggleLeftSidebar,
  onToggleRightSidebar,
  leftSidebarOpen,
  rightSidebarOpen,
}) => {
  return (
    <header className="top-toolbar fixed top-0 left-0 right-0 h-12 bg-white dark:bg-gray-900 border-b border-gray-200 dark:border-gray-700 z-50 flex items-center justify-between px-3">
      {/* Left side */}
      <div className="flex items-center gap-2">
        <button
          onClick={onToggleLeftSidebar}
          className={`toolbar-button p-2 rounded hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors ${
            leftSidebarOpen ? 'bg-gray-100 dark:bg-gray-800' : ''
          }`}
          aria-label="Toggle left sidebar"
          title="Toggle left sidebar"
        >
          <IconMenu2 size={20} className="text-gray-600 dark:text-gray-400" />
        </button>

        <button
          className="toolbar-button p-2 rounded hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
          aria-label="Search"
          title="Search (Cmd+K)"
        >
          <IconSearch size={20} className="text-gray-600 dark:text-gray-400" />
        </button>
      </div>

      {/* Right side */}
      <div className="flex items-center gap-2">
        <button
          className="toolbar-button p-2 rounded hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
          aria-label="Home"
          title="Go to home"
        >
          <IconHome size={20} className="text-gray-600 dark:text-gray-400" />
        </button>

        <button
          className="toolbar-button p-2 rounded hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors"
          aria-label="More options"
          title="More options"
        >
          <IconDots size={20} className="text-gray-600 dark:text-gray-400" />
        </button>

        <button
          onClick={onToggleRightSidebar}
          className={`toolbar-button p-2 rounded hover:bg-gray-100 dark:hover:bg-gray-800 transition-colors ${
            rightSidebarOpen ? 'bg-gray-100 dark:bg-gray-800' : ''
          }`}
          aria-label="Toggle right sidebar"
          title="Toggle right sidebar"
        >
          {rightSidebarOpen ? (
            <IconLayoutSidebarRightExpand size={20} className="text-gray-600 dark:text-gray-400" />
          ) : (
            <IconLayoutSidebarRightCollapse size={20} className="text-gray-600 dark:text-gray-400" />
          )}
        </button>
      </div>
    </header>
  );
};
