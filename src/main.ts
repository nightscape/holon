import { invoke } from '@tauri-apps/api/core';

interface Task {
  id: string;
  title: string;
  completed: boolean;
  parent_id: string | null;
  children: Task[];
}

let tasks: Task[] = [];

async function loadTasks() {
  tasks = await invoke<Task[]>('get_tasks');
  renderTasks();
}

async function addTask(title: string, parentId: string | null = null) {
  await invoke('add_task', { title, parentId });
  await loadTasks();
}

async function toggleTask(taskId: string) {
  await invoke('toggle_task', { taskId });
  await loadTasks();
}

async function deleteTask(taskId: string) {
  await invoke('delete_task', { taskId });
  await loadTasks();
}

function renderTasks() {
  const container = document.getElementById('tasks-container');
  if (!container) return;

  if (tasks.length === 0) {
    container.innerHTML = `
      <div class="empty-state">
        <h3>No tasks yet</h3>
        <p>Add your first task to get started!</p>
      </div>
    `;
    return;
  }

  container.innerHTML = tasks.map(task => renderTask(task)).join('');

  document.querySelectorAll('.task-checkbox').forEach(checkbox => {
    checkbox.addEventListener('change', (e) => {
      const target = e.target as HTMLInputElement;
      const taskId = target.dataset.taskId;
      if (taskId) {
        toggleTask(taskId);
      }
    });
  });

  document.querySelectorAll('.delete-task-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const target = e.target as HTMLButtonElement;
      const taskId = target.dataset.taskId;
      if (taskId) {
        deleteTask(taskId);
      }
    });
  });

  document.querySelectorAll('.show-add-subtask-btn').forEach(btn => {
    btn.addEventListener('click', (e) => {
      const target = e.target as HTMLButtonElement;
      const taskId = target.dataset.taskId;
      const section = document.getElementById(`add-subtask-${taskId}`);
      if (section) {
        section.style.display = section.style.display === 'none' ? 'flex' : 'none';
      }
    });
  });

  document.querySelectorAll('.add-subtask-btn').forEach(btn => {
    btn.addEventListener('click', async (e) => {
      const target = e.target as HTMLButtonElement;
      const parentId = target.dataset.parentId;
      const input = document.getElementById(`subtask-input-${parentId}`) as HTMLInputElement;

      if (input && input.value.trim() && parentId) {
        await addTask(input.value.trim(), parentId);
        input.value = '';
        const section = document.getElementById(`add-subtask-${parentId}`);
        if (section) {
          section.style.display = 'none';
        }
      }
    });
  });

  document.querySelectorAll('.subtask-input').forEach(input => {
    input.addEventListener('keypress', async (e) => {
      if (e.key === 'Enter') {
        const target = e.target as HTMLInputElement;
        const parentId = target.dataset.parentId;

        if (target.value.trim() && parentId) {
          await addTask(target.value.trim(), parentId);
          target.value = '';
          const section = document.getElementById(`add-subtask-${parentId}`);
          if (section) {
            section.style.display = 'none';
          }
        }
      }
    });
  });
}

function renderTask(task: Task): string {
  const subtasksHtml = task.children.length > 0
    ? `<div class="subtasks">${task.children.map(child => renderSubtask(child)).join('')}</div>`
    : '';

  return `
    <div class="task ${task.completed ? 'completed' : ''}">
      <div class="task-header">
        <input
          type="checkbox"
          class="task-checkbox"
          data-task-id="${task.id}"
          ${task.completed ? 'checked' : ''}
        />
        <span class="task-content">${escapeHtml(task.title)}</span>
        <div class="task-actions">
          <button class="btn btn-small btn-secondary show-add-subtask-btn" data-task-id="${task.id}">
            Add Subtask
          </button>
          <button class="btn btn-small btn-danger delete-task-btn" data-task-id="${task.id}">
            Delete
          </button>
        </div>
      </div>
      ${subtasksHtml}
      <div id="add-subtask-${task.id}" class="add-subtask-section" style="display: none;">
        <input
          type="text"
          id="subtask-input-${task.id}"
          class="subtask-input"
          data-parent-id="${task.id}"
          placeholder="Enter subtask title..."
        />
        <button class="btn btn-small btn-primary add-subtask-btn" data-parent-id="${task.id}">
          Add
        </button>
      </div>
    </div>
  `;
}

function renderSubtask(task: Task): string {
  const nestedSubtasks = task.children.length > 0
    ? `<div class="subtasks">${task.children.map(child => renderSubtask(child)).join('')}</div>`
    : '';

  return `
    <div class="subtask ${task.completed ? 'completed' : ''}">
      <input
        type="checkbox"
        class="task-checkbox"
        data-task-id="${task.id}"
        ${task.completed ? 'checked' : ''}
      />
      <span class="task-content">${escapeHtml(task.title)}</span>
      <div style="margin-left: auto; display: flex; gap: 8px;">
        <button class="btn btn-small btn-secondary show-add-subtask-btn" data-task-id="${task.id}">
          Add Subtask
        </button>
        <button class="btn btn-small btn-danger delete-task-btn" data-task-id="${task.id}">
          Delete
        </button>
      </div>
    </div>
    ${nestedSubtasks}
    <div id="add-subtask-${task.id}" class="add-subtask-section" style="display: none;">
      <input
        type="text"
        id="subtask-input-${task.id}"
        class="subtask-input"
        data-parent-id="${task.id}"
        placeholder="Enter subtask title..."
      />
      <button class="btn btn-small btn-primary add-subtask-btn" data-parent-id="${task.id}">
        Add
      </button>
    </div>
  `;
}

function escapeHtml(text: string): string {
  const div = document.createElement('div');
  div.textContent = text;
  return div.innerHTML;
}

document.addEventListener('DOMContentLoaded', () => {
  const addTaskBtn = document.getElementById('add-task-btn');
  const newTaskInput = document.getElementById('new-task-input') as HTMLInputElement;

  addTaskBtn?.addEventListener('click', async () => {
    if (newTaskInput && newTaskInput.value.trim()) {
      await addTask(newTaskInput.value.trim());
      newTaskInput.value = '';
    }
  });

  newTaskInput?.addEventListener('keypress', async (e) => {
    if (e.key === 'Enter' && newTaskInput.value.trim()) {
      await addTask(newTaskInput.value.trim());
      newTaskInput.value = '';
    }
  });

  loadTasks();
});
