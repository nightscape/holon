/**
 * Fake Backend Implementation
 *
 * This is a complete in-memory implementation of the backend IPC interface.
 * It can be used for:
 * - Browser development mode (with localStorage persistence)
 * - Frontend unit tests
 * - Playwright E2E tests
 *
 * This is NOT a mock - it's a real alternative backend with actual business logic.
 */

interface Task {
  id: string;
  title: string;
  parent_id: string | null;
  children: string[];
  created_at?: number;
  updated_at?: number;
}

class MockDatabase {
  private tasks: Map<string, Task> = new Map();
  private idCounter = 1;
  private persistToStorage = true;

  constructor() {
    this.loadFromStorage();
  }

  private loadFromStorage() {
    if (!this.persistToStorage) return;

    try {
      const stored = localStorage.getItem('mock-tasks');
      if (stored) {
        const tasks = JSON.parse(stored) as Task[];
        tasks.forEach(task => this.tasks.set(task.id, task));
        const maxId = tasks.reduce((max, task) => {
          const num = parseInt(task.id);
          return isNaN(num) ? max : Math.max(max, num);
        }, 0);
        this.idCounter = maxId + 1;
      }
    } catch (error) {
      console.warn('Failed to load mock tasks from storage:', error);
    }
  }

  private saveToStorage() {
    if (!this.persistToStorage) return;

    try {
      const tasks = Array.from(this.tasks.values());
      localStorage.setItem('mock-tasks', JSON.stringify(tasks));
    } catch (error) {
      console.warn('Failed to save mock tasks to storage:', error);
    }
  }

  /**
   * Enable or disable localStorage persistence
   * Useful for tests where you don't want cross-test pollution
   */
  setPersistence(enabled: boolean) {
    this.persistToStorage = enabled;
  }

  getTasks(): Task[] {
    return Array.from(this.tasks.values());
  }

  addTask(title: string, parentId: string | null = null, index?: number): Task {
    const id = String(this.idCounter++);
    const task: Task = {
      id,
      title,
      parent_id: parentId,
      children: [],
      created_at: Date.now(),
      updated_at: Date.now(),
    };

    this.tasks.set(id, task);

    if (parentId && this.tasks.has(parentId)) {
      const parent = this.tasks.get(parentId)!;
      if (!parent.children.includes(id)) {
        if (index !== undefined && index >= 0 && index <= parent.children.length) {
          parent.children.splice(index, 0, id);
        } else {
          parent.children.push(id);
        }
      }
    }

    this.saveToStorage();
    return task;
  }

  updateTask(taskId: string, title: string): boolean {
    const task = this.tasks.get(taskId);
    if (!task) return false;

    task.title = title;
    task.updated_at = Date.now();
    this.saveToStorage();
    return true;
  }

  deleteTask(taskId: string): boolean {
    const task = this.tasks.get(taskId);
    if (!task) return false;

    if (task.parent_id && this.tasks.has(task.parent_id)) {
      const parent = this.tasks.get(task.parent_id)!;
      parent.children = parent.children.filter(id => id !== taskId);
    }

    task.children.forEach(childId => {
      this.deleteTask(childId);
    });

    this.tasks.delete(taskId);
    this.saveToStorage();
    return true;
  }

  moveTask(taskId: string, newParentId: string | null, index: number): boolean {
    const task = this.tasks.get(taskId);
    if (!task) return false;

    if (task.parent_id && this.tasks.has(task.parent_id)) {
      const oldParent = this.tasks.get(task.parent_id)!;
      oldParent.children = oldParent.children.filter(id => id !== taskId);
    }

    task.parent_id = newParentId;

    if (newParentId && this.tasks.has(newParentId)) {
      const newParent = this.tasks.get(newParentId)!;
      newParent.children.splice(index, 0, taskId);
    }

    this.saveToStorage();
    return true;
  }

  clearAll() {
    this.tasks.clear();
    this.idCounter = 1;
    this.saveToStorage();
  }

  seedSampleData() {
    this.clearAll();

    const root1 = this.addTask('Welcome to Rusty Knowledge! 👋', null);
    const root2 = this.addTask('Getting Started', null);
    const root3 = this.addTask('Features', null);

    this.addTask('This is a demo running in browser mode', root1.id);
    this.addTask('All data is stored in localStorage', root1.id);

    this.addTask('Create new blocks by clicking the "+ Add block" button', root2.id);
    this.addTask('Edit blocks by clicking on them', root2.id);
    this.addTask('Delete blocks using the × button', root2.id);

    this.addTask('Hierarchical organization', root3.id);
    this.addTask('Drag and drop (coming soon)', root3.id);
    this.addTask('Full-text search (coming soon)', root3.id);
  }
}

const database = new MockDatabase();

/**
 * Fake backend invoke function
 * This implements the same IPC interface as the real Tauri backend
 */
async function invoke<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  console.log(`[Fake Backend] ${cmd}`, args);

  // Simulate network delay
  await new Promise(resolve => setTimeout(resolve, 50));

  switch (cmd) {
    case 'get_tasks':
      return database.getTasks() as T;

    case 'add_task': {
      const { title, parentId, index } = args as { title: string; parentId?: string | null; index?: number };
      return database.addTask(title, parentId ?? null, index) as T;
    }

    case 'update_task': {
      const { taskId, title } = args as { taskId: string; title: string };
      const success = database.updateTask(taskId, title);
      if (!success) throw new Error(`Task ${taskId} not found`);
      return success as T;
    }

    case 'delete_task': {
      const { taskId } = args as { taskId: string };
      const success = database.deleteTask(taskId);
      if (!success) throw new Error(`Task ${taskId} not found`);
      return success as T;
    }

    case 'move_task': {
      const { taskId, newParentId, index } = args as {
        taskId: string;
        newParentId: string | null;
        index: number;
      };
      const success = database.moveTask(taskId, newParentId, index);
      if (!success) throw new Error(`Task ${taskId} not found`);
      return success as T;
    }

    default:
      throw new Error(`Unknown command: ${cmd}`);
  }
}

/**
 * Command handler type for temporary overrides
 */
type CommandHandler<T = any> = (args?: Record<string, unknown>) => T | Promise<T>;

/**
 * Temporary command overrides for testing error cases
 */
const commandOverrides = new Map<string, CommandHandler>();

/**
 * Modified invoke function that checks for overrides first
 */
async function invokeWithOverrides<T>(cmd: string, args?: Record<string, unknown>): Promise<T> {
  // Check if there's a temporary override for this command
  if (commandOverrides.has(cmd)) {
    const handler = commandOverrides.get(cmd)!;
    return handler(args) as Promise<T>;
  }

  // Otherwise use the normal invoke
  return invoke<T>(cmd, args);
}

/**
 * Fake Backend API
 * This is the public interface used by the application and tests
 */
export const fakeBackend = {
  /** Main invoke function - implements the IPC interface */
  invoke: invokeWithOverrides,

  /** Direct access to the database for testing/debugging */
  database,

  /** Clear all data */
  clearAll: () => database.clearAll(),

  /** Seed sample data */
  seedSample: () => database.seedSampleData(),

  /** Enable/disable localStorage persistence (useful for tests) */
  setPersistence: (enabled: boolean) => database.setPersistence(enabled),

  /**
   * Temporarily override a command handler for testing
   *
   * @example
   * ```typescript
   * await fakeBackend.withTemporaryOverride(
   *   'add_task',
   *   () => { throw new Error('Failed to add task'); },
   *   async () => {
   *     await store.addBlock('Test', null);
   *     expect(store.error).toBe('Error: Failed to add task');
   *   }
   * );
   * ```
   */
  async withTemporaryOverride<T>(
    command: string,
    handler: CommandHandler<T>,
    testFn: () => void | Promise<void>
  ): Promise<void> {
    commandOverrides.set(command, handler);
    try {
      await testFn();
    } finally {
      commandOverrides.delete(command);
    }
  },
};

// Legacy exports for backward compatibility (will be removed)
/** @deprecated Use fakeBackend.invoke instead */
export const mockInvoke = invoke;

/** @deprecated Use fakeBackend.clearAll instead */
export function clearMockData() {
  database.clearAll();
}

/** @deprecated Use fakeBackend.seedSample instead */
export function seedMockData() {
  database.seedSampleData();
}
