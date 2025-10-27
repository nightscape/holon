import { create } from 'zustand';
import { Block, BlockNode } from '../types/block';
import { invoke } from '@tauri-apps/api/core';
import { isBrowserMode } from '../lib/env';
import { fakeBackend } from '../lib/fakeBackend';

interface BlockStore {
  blocks: Block[];
  loading: boolean;
  error: string | null;
  lastCreatedBlockId: string | null;

  loadBlocks: () => Promise<void>;
  addBlock: (content: string, parentId?: string | null, index?: number) => Promise<string | null>;
  updateBlock: (id: string, content: string) => Promise<void>;
  deleteBlock: (id: string) => Promise<void>;
  moveBlock: (id: string, newParentId: string | null, index: number) => Promise<void>;
  clearLastCreatedBlockId: () => void;

  blocksToTree: () => BlockNode[];
}

async function smartInvoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  if (isBrowserMode()) {
    return fakeBackend.invoke<T>(cmd, args);
  }
  return invoke<T>(cmd, args);
}

export const useBlockStore = create<BlockStore>((set, get) => ({
  blocks: [],
  loading: false,
  error: null,
  lastCreatedBlockId: null,

  loadBlocks: async () => {
    set({ loading: true, error: null });
    try {
      const tasks = await smartInvoke<any[]>('get_tasks');
      const blocks: Block[] = tasks.map(task => ({
        id: task.id,
        content: task.title,
        parentId: task.parent_id,
        children: task.children || [],
        collapsed: false,
        createdAt: Date.now(),
        updatedAt: Date.now(),
      }));
      set({ blocks, loading: false });
    } catch (error) {
      set({ error: String(error), loading: false });
    }
  },

  addBlock: async (content: string, parentId: string | null = null, index?: number) => {
    try {
      const task = await smartInvoke<any>('add_task', { title: content, parentId, index });
      const newBlockId = task.id;
      await get().loadBlocks();
      set({ lastCreatedBlockId: newBlockId });
      return newBlockId;
    } catch (error) {
      set({ error: String(error) });
      return null;
    }
  },

  clearLastCreatedBlockId: () => {
    set({ lastCreatedBlockId: null });
  },

  updateBlock: async (id: string, content: string) => {
    try {
      // Optimistically update local state first
      set(state => ({
        blocks: state.blocks.map(block =>
          block.id === id ? { ...block, content, updatedAt: Date.now() } : block
        )
      }));

      // Then update backend
      await smartInvoke('update_task', { taskId: id, title: content });
    } catch (error) {
      set({ error: String(error) });
      // Reload on error to revert optimistic update
      await get().loadBlocks();
    }
  },

  deleteBlock: async (id: string) => {
    try {
      await smartInvoke('delete_task', { taskId: id });
      await get().loadBlocks();
    } catch (error) {
      set({ error: String(error) });
    }
  },

  moveBlock: async (id: string, newParentId: string | null, index: number) => {
    try {
      await smartInvoke('move_task', { taskId: id, newParentId, index });
      await get().loadBlocks();
    } catch (error) {
      set({ error: String(error) });
    }
  },

  blocksToTree: () => {
    const blocks = get().blocks;
    const blockMap = new Map<string, BlockNode>();
    const rootBlocks: BlockNode[] = [];

    // First pass: create all nodes
    blocks.forEach(block => {
      blockMap.set(block.id, {
        id: block.id,
        name: block.content || 'Empty block',
        children: [],
        data: block,
      });
    });

    // Second pass: build hierarchy using the children array order from backend
    blocks.forEach(block => {
      const node = blockMap.get(block.id)!;

      if (block.parentId) {
        // This block has a parent - it will be added to parent's children via parent's children array
        // Don't add it here, let the parent handle it
      } else {
        // Root level block
        rootBlocks.push(node);
      }

      // Add children in the order specified by block.children array
      if (block.children && block.children.length > 0) {
        node.children = block.children
          .map(childId => blockMap.get(childId))
          .filter((child): child is BlockNode => child !== undefined);
      }
    });

    return rootBlocks;
  },
}));
