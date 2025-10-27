import React, { useState } from 'react';
import { TopToolbar } from './TopToolbar';
import { LeftSidebar } from './LeftSidebar';

interface MainLayoutProps {
  children: React.ReactNode;
}

export const MainLayout: React.FC<MainLayoutProps> = ({ children }) => {
  const [leftSidebarOpen, setLeftSidebarOpen] = useState(true);
  const [rightSidebarOpen, setRightSidebarOpen] = useState(false);

  return (
    <div className="main-layout min-h-screen bg-white dark:bg-gray-900">
      <TopToolbar
        onToggleLeftSidebar={() => setLeftSidebarOpen(!leftSidebarOpen)}
        onToggleRightSidebar={() => setRightSidebarOpen(!rightSidebarOpen)}
        leftSidebarOpen={leftSidebarOpen}
        rightSidebarOpen={rightSidebarOpen}
      />

      <LeftSidebar isOpen={leftSidebarOpen} />

      <main
        className={`main-content transition-all duration-200 ${
          leftSidebarOpen ? 'ml-60' : 'ml-0'
        } ${rightSidebarOpen ? 'mr-80' : 'mr-0'} pt-12`}
        style={{ minHeight: 'calc(100vh - 48px)' }}
      >
        <div className="max-w-4xl mx-auto p-8">
          {children}
        </div>
      </main>

      {/* Right sidebar placeholder */}
      {rightSidebarOpen && (
        <aside className="right-sidebar fixed right-0 top-12 bottom-0 w-80 bg-gray-50 dark:bg-gray-800 border-l border-gray-200 dark:border-gray-700 overflow-y-auto z-40">
          <div className="p-4">
            <h3 className="text-sm font-semibold text-gray-500 dark:text-gray-400 mb-4">
              Right Sidebar
            </h3>
            <p className="text-sm text-gray-600 dark:text-gray-400">
              Placeholder for additional content, properties, references, etc.
            </p>
          </div>
        </aside>
      )}
    </div>
  );
};
