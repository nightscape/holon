import React, { useEffect, useRef } from 'react';
import { Tree, NodeRendererProps } from 'react-arborist';
import { IconChevronRight, IconChevronDown, IconX } from '@tabler/icons-react';
import { BlockEditor } from './BlockEditor';
import { useBlockStore } from '../store/blockStore';
import { BlockNode } from '../types/block';
import '../styles/outliner.css';

const BlockNodeRenderer: React.FC<NodeRendererProps<BlockNode>> = React.memo(({ node, style, dragHandle }) => {
  const { updateBlock, deleteBlock, addBlock, moveBlock, blocks, lastCreatedBlockId, clearLastCreatedBlockId } = useBlockStore();
  const updateTimeoutRef = React.useRef<NodeJS.Timeout>();

  // Destructure the actual block data to avoid .data.data access
  const block = node.data.data;
  const blockId = block.id;
  const blockParentId = block.parentId;
  const blockChildren = block.children;

  const [localContent, setLocalContent] = React.useState(block.content);
  const [isEditing, setIsEditing] = React.useState(false);
  const shouldAutoFocus = lastCreatedBlockId === blockId;

  // Clear the auto-focus flag after it's been used
  React.useEffect(() => {
    if (shouldAutoFocus) {
      clearLastCreatedBlockId();
    }
  }, [shouldAutoFocus, clearLastCreatedBlockId]);

  console.log('[BlockNodeRenderer] Rendering node:', blockId);

  // Update local content when node data changes (but not while typing)
  React.useEffect(() => {
    console.log('[BlockNodeRenderer] Node content changed from external source:', blockId);
    setLocalContent(block.content);
  }, [block.content, blockId]);

  React.useEffect(() => {
    console.log('[BlockNodeRenderer] Mounted:', blockId);
    return () => {
      console.log('[BlockNodeRenderer] Unmounted:', blockId);
    };
  }, [blockId]);

  const handleUpdate = (content: string) => {
    console.log('[BlockNodeRenderer] handleUpdate called:', content);
    // Update local state immediately for responsive UI
    setLocalContent(content);

    // Debounce backend updates
    if (updateTimeoutRef.current) {
      clearTimeout(updateTimeoutRef.current);
    }

    updateTimeoutRef.current = setTimeout(() => {
      console.log('[BlockNodeRenderer] Calling backend updateBlock');
      updateBlock(blockId, content);
    }, 1000); // Wait 1 second after last keystroke
  };

  const handleEnter = async () => {
    console.log('[BlockNodeRenderer] Enter pressed, creating new block as sibling');

    // Find the current block's position among its siblings
    const siblings = blocks.filter(b => b.parentId === blockParentId);
    const currentIndex = siblings.findIndex(b => b.id === blockId);

    // Insert new block right after the current one
    await addBlock('', blockParentId, currentIndex + 1);
  };

  const handleTab = () => {
    console.log('[BlockNodeRenderer] Tab pressed, indenting block under previous sibling');

    // Find siblings (blocks with same parent) - get fresh blocks from store
    const currentBlocks = useBlockStore.getState().blocks;
    const siblings = currentBlocks.filter(b => b.parentId === blockParentId);
    const currentIndex = siblings.findIndex(b => b.id === blockId);

    console.log('[BlockNodeRenderer] Found', siblings.length, 'siblings for parentId', blockParentId, ', current index:', currentIndex);

    if (currentIndex > 0) {
      // The previous sibling becomes the new parent
      const previousSibling = siblings[currentIndex - 1];
      console.log('[BlockNodeRenderer] Moving block under previous sibling:', previousSibling.id);
      // Add as the last child of the previous sibling
      moveBlock(blockId, previousSibling.id, previousSibling.children.length);
    } else {
      console.log('[BlockNodeRenderer] Cannot indent - no previous sibling (currentIndex:', currentIndex, ')');
    }
  };

  const handleFocusChange = (focused: boolean) => {
    console.log('[BlockNodeRenderer] Focus changed:', focused);
    setIsEditing(focused);
  };

  const handleBackspace = () => {
    if (!localContent || localContent.trim() === '') {
      deleteBlock(blockId);
    }
  };

  const hasChildren = node.children && node.children.length > 0;

  return (
    <div
      style={style}
      className={`ls-block group flex items-start gap-2 py-1 px-2 ${isEditing ? 'editing' : ''}`}
      ref={dragHandle}
    >
      {/* Bullet container */}
      <button
        onClick={(e) => {
          e.stopPropagation();
          node.toggle();
        }}
        className="bullet-container mt-1"
        aria-label={hasChildren ? (node.isOpen ? 'Collapse' : 'Expand') : 'Bullet'}
      >
        {hasChildren ? (
          node.isOpen ? (
            <IconChevronDown size={12} className="text-gray-600 dark:text-gray-400" />
          ) : (
            <IconChevronRight size={12} className="text-gray-600 dark:text-gray-400" />
          )
        ) : (
          <span className="bullet" />
        )}
      </button>

      {/* Content */}
      <div className="flex-1 min-w-0 block-content-or-editor-inner">
        <BlockEditor
          key={node.id}
          content={localContent}
          onUpdate={handleUpdate}
          onEnter={handleEnter}
          onBackspace={handleBackspace}
          onTab={handleTab}
          onFocusChange={handleFocusChange}
          autoFocus={shouldAutoFocus}
        />
      </div>

      {/* Delete button */}
      <button
        onClick={(e) => {
          e.stopPropagation();
          deleteBlock(node.data.id);
        }}
        className="block-control group-hover-visible text-gray-400 hover:text-red-600 dark:hover:text-red-400 mt-1"
        aria-label="Delete block"
      >
        <IconX size={16} />
      </button>
    </div>
  );
}, (prev, next) => {
  const prevBlock = prev.node.data.data;
  const nextBlock = next.node.data.data;
  const shouldSkipRender =
    prev.node.id === next.node.id &&
    prevBlock.content === nextBlock.content &&
    prevBlock.parentId === nextBlock.parentId &&
    prevBlock.children.length === nextBlock.children.length;
  console.log('[BlockNodeRenderer] Memo check:', prev.node.id, 'Skip render:', shouldSkipRender);
  return shouldSkipRender;
});

export const OutlinerTree: React.FC = () => {
  const { blocks, loadBlocks, addBlock, blocksToTree } = useBlockStore();
  const treeRef = useRef<any>(null);

  useEffect(() => {
    loadBlocks();
  }, [loadBlocks]);

  const treeData = blocksToTree();

  const handleAddRootBlock = () => {
    addBlock('New block', null);
  };

  return (
    <div className="outliner-container">
      {treeData.length === 0 ? (
        <div className="outliner-empty-state flex flex-col items-center justify-center py-12">
          <div className="text-center mb-4">
            <p className="text-lg text-gray-600 dark:text-gray-400">No blocks yet</p>
            <p className="text-sm text-gray-500 dark:text-gray-500">
              Click below to create your first block
            </p>
          </div>
          <button
            onClick={handleAddRootBlock}
            className="px-4 py-2 bg-blue-600 text-white rounded-md hover:bg-blue-700 transition-colors text-sm font-medium"
          >
            Add Block
          </button>
        </div>
      ) : (
        <>
          <Tree
            ref={treeRef}
            data={treeData}
            openByDefault={true}
            width="100%"
            height={600}
            indent={29}
            rowHeight={36}
            overscanCount={10}
          >
            {BlockNodeRenderer}
          </Tree>

          <button
            onClick={handleAddRootBlock}
            className="mt-4 text-sm text-blue-500 hover:text-blue-600 dark:text-blue-400 dark:hover:text-blue-300 transition-colors"
          >
            + Add block
          </button>
        </>
      )}
    </div>
  );
};
