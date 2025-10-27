import { describe, it, expect, beforeEach } from 'vitest';
import { useBlockStore } from './blockStore';
import { fakeBackend } from '../lib/fakeBackend';

describe('blockStore', () => {
  beforeEach(() => {
    // Reset store state
    const store = useBlockStore.getState();
    store.blocks = [];
    store.loading = false;
    store.error = null;

    // Clear fake backend (already done in test/setup.ts, but explicit here)
    fakeBackend.clearAll();
  });

  describe('loadBlocks', () => {
    it('should load blocks from backend', async () => {
      // Seed the fake backend with data
      fakeBackend.database.addTask('Task 1', null);
      fakeBackend.database.addTask('Task 2', '1');

      await useBlockStore.getState().loadBlocks();

      const state = useBlockStore.getState();
      expect(state.blocks).toHaveLength(2);
      expect(state.blocks[0].id).toBe('1');
      expect(state.blocks[0].content).toBe('Task 1');
      expect(state.blocks[0].parentId).toBe(null);
      expect(state.blocks[1].id).toBe('2');
      expect(state.blocks[1].content).toBe('Task 2');
      expect(state.blocks[1].parentId).toBe('1');
      expect(state.loading).toBe(false);
    });

    it('should set loading state during fetch', async () => {
      const store = useBlockStore.getState();
      const loadPromise = store.loadBlocks();

      // Check loading state is true while loading
      const currentState = useBlockStore.getState();
      expect(currentState.loading).toBe(true);

      await loadPromise;

      expect(useBlockStore.getState().loading).toBe(false);
    });

    it('should handle errors during load', async () => {
      await fakeBackend.withTemporaryOverride(
        'get_tasks',
        () => { throw new Error('Failed to load tasks'); },
        async () => {
          await useBlockStore.getState().loadBlocks();

          const state = useBlockStore.getState();
          expect(state.error).toBe('Error: Failed to load tasks');
          expect(state.loading).toBe(false);
        }
      );
    });
  });

  describe('addBlock', () => {
    it('should add a root block', async () => {
      await useBlockStore.getState().addBlock('New block', null);

      const state = useBlockStore.getState();
      expect(state.blocks).toHaveLength(1);
      expect(state.blocks[0].content).toBe('New block');
      expect(state.blocks[0].parentId).toBe(null);

      // Verify it's in the fake backend
      const tasks = fakeBackend.database.getTasks();
      expect(tasks).toHaveLength(1);
      expect(tasks[0].title).toBe('New block');
    });

    it('should add a child block', async () => {
      // Add parent first
      await useBlockStore.getState().addBlock('Parent', null);
      const parentId = useBlockStore.getState().blocks[0].id;

      // Add child
      await useBlockStore.getState().addBlock('Child', parentId);

      const state = useBlockStore.getState();
      expect(state.blocks).toHaveLength(2);
      expect(state.blocks[1].content).toBe('Child');
      expect(state.blocks[1].parentId).toBe(parentId);
    });

    it('should handle errors during add', async () => {
      await fakeBackend.withTemporaryOverride(
        'add_task',
        () => { throw new Error('Failed to add task'); },
        async () => {
          await useBlockStore.getState().addBlock('Test', null);

          const state = useBlockStore.getState();
          expect(state.error).toBe('Error: Failed to add task');
        }
      );
    });
  });

  describe('updateBlock', () => {
    it('should update block content', async () => {
      // Add a block first
      fakeBackend.database.addTask('Original', null);
      await useBlockStore.getState().loadBlocks();

      // Update it
      await useBlockStore.getState().updateBlock('1', 'Updated content');

      const state = useBlockStore.getState();
      expect(state.blocks[0].content).toBe('Updated content');

      // Verify in fake backend
      const tasks = fakeBackend.database.getTasks();
      expect(tasks[0].title).toBe('Updated content');
    });

    it('should handle errors during update', async () => {
      await fakeBackend.withTemporaryOverride(
        'update_task',
        () => { throw new Error('Failed to update task'); },
        async () => {
          await useBlockStore.getState().updateBlock('1', 'New content');

          const state = useBlockStore.getState();
          expect(state.error).toBe('Error: Failed to update task');
        }
      );
    });
  });

  describe('deleteBlock', () => {
    it('should delete a block', async () => {
      // Add a block
      fakeBackend.database.addTask('To Delete', null);
      await useBlockStore.getState().loadBlocks();

      expect(useBlockStore.getState().blocks).toHaveLength(1);

      // Delete it
      await useBlockStore.getState().deleteBlock('1');

      const state = useBlockStore.getState();
      expect(state.blocks).toHaveLength(0);

      // Verify in fake backend
      const tasks = fakeBackend.database.getTasks();
      expect(tasks).toHaveLength(0);
    });

    it('should handle errors during delete', async () => {
      await fakeBackend.withTemporaryOverride(
        'delete_task',
        () => { throw new Error('Failed to delete task'); },
        async () => {
          await useBlockStore.getState().deleteBlock('1');

          const state = useBlockStore.getState();
          expect(state.error).toBe('Error: Failed to delete task');
        }
      );
    });
  });

  describe('moveBlock', () => {
    it('should move a block to new parent', async () => {
      // Add blocks
      fakeBackend.database.addTask('Parent', null);
      fakeBackend.database.addTask('Child', null);
      await useBlockStore.getState().loadBlocks();

      // Move child under parent
      await useBlockStore.getState().moveBlock('2', '1', 0);

      const state = useBlockStore.getState();
      const child = state.blocks.find(b => b.id === '2');
      expect(child?.parentId).toBe('1');

      // Verify in fake backend
      const tasks = fakeBackend.database.getTasks();
      const childTask = tasks.find(t => t.id === '2');
      expect(childTask?.parent_id).toBe('1');
    });

    it('should handle errors during move', async () => {
      await fakeBackend.withTemporaryOverride(
        'move_task',
        () => { throw new Error('Failed to move task'); },
        async () => {
          await useBlockStore.getState().moveBlock('1', 'parent-1', 0);

          const state = useBlockStore.getState();
          expect(state.error).toBe('Error: Failed to move task');
        }
      );
    });
  });

  describe('blocksToTree', () => {
    it('should convert flat blocks to tree structure', () => {
      const store = useBlockStore.getState();
      store.blocks = [
        { id: '1', content: 'Parent', parentId: null, children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
        { id: '2', content: 'Child 1', parentId: '1', children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
        { id: '3', content: 'Child 2', parentId: '1', children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
        { id: '4', content: 'Root 2', parentId: null, children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
      ];

      const tree = store.blocksToTree();

      expect(tree).toHaveLength(2);
      expect(tree[0].id).toBe('1');
      expect(tree[0].name).toBe('Parent');
      expect(tree[0].children).toHaveLength(2);
      expect(tree[0].children![0].id).toBe('2');
      expect(tree[0].children![1].id).toBe('3');
      expect(tree[1].id).toBe('4');
      expect(tree[1].children).toHaveLength(0);
    });

    it('should handle empty blocks', () => {
      const store = useBlockStore.getState();
      store.blocks = [];

      const tree = store.blocksToTree();

      expect(tree).toHaveLength(0);
    });

    it('should use "Empty block" for blocks without content', () => {
      const store = useBlockStore.getState();
      store.blocks = [
        { id: '1', content: '', parentId: null, children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
      ];

      const tree = store.blocksToTree();

      expect(tree[0].name).toBe('Empty block');
    });

    it('should handle orphaned blocks (parent not in list)', () => {
      const store = useBlockStore.getState();
      store.blocks = [
        { id: '1', content: 'Orphan', parentId: 'non-existent', children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
      ];

      const tree = store.blocksToTree();

      expect(tree).toHaveLength(1);
      expect(tree[0].id).toBe('1');
    });

    it('should handle deep nesting', () => {
      const store = useBlockStore.getState();
      store.blocks = [
        { id: '1', content: 'Level 1', parentId: null, children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
        { id: '2', content: 'Level 2', parentId: '1', children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
        { id: '3', content: 'Level 3', parentId: '2', children: [], collapsed: false, createdAt: 0, updatedAt: 0 },
      ];

      const tree = store.blocksToTree();

      expect(tree).toHaveLength(1);
      expect(tree[0].id).toBe('1');
      expect(tree[0].children![0].id).toBe('2');
      expect(tree[0].children![0].children![0].id).toBe('3');
    });
  });
});
