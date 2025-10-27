import React from 'react';
import { IconPhoto, IconSettings } from '@tabler/icons-react';

interface PageTitleProps {
  title: string;
  tag?: string;
  onAddIcon?: () => void;
  onSetProperty?: () => void;
}

export const PageTitle: React.FC<PageTitleProps> = ({
  title,
  tag,
  onAddIcon,
  onSetProperty,
}) => {
  return (
    <div className="page-title-container mb-8">
      <div className="flex items-center gap-2 mb-2">
        <button
          onClick={onAddIcon}
          className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors"
          aria-label="Add icon"
          title="Add icon"
        >
          <IconPhoto size={16} />
        </button>

        <button
          onClick={onSetProperty}
          className="p-1.5 rounded hover:bg-gray-100 dark:hover:bg-gray-800 text-gray-400 hover:text-gray-600 dark:hover:text-gray-300 transition-colors text-xs"
          aria-label="Set property"
          title="Set property"
        >
          <IconSettings size={16} />
        </button>
      </div>

      <div className="flex items-baseline justify-between">
        <h1 className="text-3xl font-bold text-gray-900 dark:text-gray-100">
          {title}
        </h1>

        {tag && (
          <a
            href={`#/tag/${tag}`}
            className="text-sm text-blue-500 hover:text-blue-600 dark:text-blue-400 dark:hover:text-blue-300 transition-colors"
          >
            #{tag}
          </a>
        )}
      </div>
    </div>
  );
};
