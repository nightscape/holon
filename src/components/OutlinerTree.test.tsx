import { describe, it, expect, beforeEach, vi } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import React from 'react';
import { OutlinerTree } from './OutlinerTree';
import { useBlockStore } from '../store/blockStore';
import { fakeBackend } from '../lib/fakeBackend';

vi.mock('./BlockEditor', () => ({
  BlockEditor: ({ content, onUpdate, onEnter, onTab, onFocusChange }: any) => {
    const [isFocused, setIsFocused] = React.useState(false);

    const handleChange = (e: React.ChangeEvent<HTMLInputElement>) => {
      onUpdate(e.target.value);
    };

    const handleKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
      if (e.key === 'Enter' && onEnter) {
        e.preventDefault();
        onEnter();
      }
      if (e.key === 'Tab' && onTab) {
        e.preventDefault();
        onTab();
      }
    };

    const handleFocus = (e: React.FocusEvent<HTMLInputElement>) => {
      setIsFocused(true);
      if (onFocusChange) {
        onFocusChange(true);
      }
    };

    const handleBlur = (e: React.FocusEvent<HTMLInputElement>) => {
      setIsFocused(false);
      if (onFocusChange) {
        onFocusChange(false);
      }
    };

    return (
      <div data-testid="block-editor" data-content={content} data-focused={isFocused}>
        <input
          onChange={handleChange}
          onKeyDown={handleKeyDown}
          onFocus={handleFocus}
          onBlur={handleBlur}
        />
      </div>
    );
  },
}));

describe('OutlinerTree', () => {
  beforeEach(() => {
    const store = useBlockStore.getState();
    store.blocks = [];
    store.loading = false;
    store.error = null;
    fakeBackend.clearAll();
  });

  it('should show empty state when no blocks exist', async () => {
    render(<OutlinerTree />);

    await waitFor(() => {
      expect(screen.getByText('No blocks yet')).toBeInTheDocument();
      expect(screen.getByText('Click below to create your first block')).toBeInTheDocument();
    });
  });

  it('should allow adding first block from empty state', async () => {
    render(<OutlinerTree />);

    await waitFor(() => {
      expect(screen.getByText('No blocks yet')).toBeInTheDocument();
    });

    const addButton = screen.getByRole('button', { name: /add block/i });
    await userEvent.click(addButton);

    await waitFor(() => {
      expect(screen.queryByText('No blocks yet')).not.toBeInTheDocument();
    });
  });

  it('should render tree with blocks', async () => {
    fakeBackend.database.addTask('First block', null);
    fakeBackend.database.addTask('Second block', null);

    render(<OutlinerTree />);

    await waitFor(() => {
      const editors = screen.getAllByTestId('block-editor');
      expect(editors).toHaveLength(2);
    });
  });

  it('should render hierarchical blocks', async () => {
    fakeBackend.database.addTask('Parent', null);
    fakeBackend.database.addTask('Child 1', '1');
    fakeBackend.database.addTask('Child 2', '1');

    render(<OutlinerTree />);

    await waitFor(() => {
      const editors = screen.getAllByTestId('block-editor');
      expect(editors).toHaveLength(3);
    });
  });

  it('should allow adding blocks when tree exists', async () => {
    fakeBackend.database.addTask('Existing', null);

    render(<OutlinerTree />);

    await waitFor(() => {
      expect(screen.getAllByTestId('block-editor')).toHaveLength(1);
    });

    const addButton = screen.getByRole('button', { name: /add block/i });
    await userEvent.click(addButton);

    await waitFor(() => {
      expect(screen.getAllByTestId('block-editor')).toHaveLength(2);
    });
  });

  it('should allow deleting blocks', async () => {
    fakeBackend.database.addTask('Block to delete', null);

    render(<OutlinerTree />);

    await waitFor(() => {
      expect(screen.getByTestId('block-editor')).toBeInTheDocument();
    });

    const deleteButton = screen.getByRole('button', { name: /delete block/i });
    await userEvent.click(deleteButton);

    await waitFor(() => {
      expect(screen.getByText('No blocks yet')).toBeInTheDocument();
    });
  });

  it('should show collapse/expand buttons for parent nodes', async () => {
    fakeBackend.database.addTask('Parent', null);
    fakeBackend.database.addTask('Child', '1');

    render(<OutlinerTree />);

    await waitFor(() => {
      const collapseButton = screen.getByRole('button', { name: /collapse/i });
      expect(collapseButton).toBeInTheDocument();
    });
  });

  it('should show bullet for leaf nodes', async () => {
    fakeBackend.database.addTask('Leaf node', null);

    render(<OutlinerTree />);

    await waitFor(() => {
      const bulletButton = screen.getByRole('button', { name: /bullet/i });
      expect(bulletButton).toBeInTheDocument();
    });
  });

  it('should render block editors', async () => {
    fakeBackend.database.addTask('Original', null);

    render(<OutlinerTree />);

    await waitFor(() => {
      expect(screen.getByTestId('block-editor')).toBeInTheDocument();
      expect(screen.getByRole('textbox')).toBeInTheDocument();
    });
  });

  it('should create new block when Enter is pressed', async () => {
    fakeBackend.database.addTask('First block', null);

    render(<OutlinerTree />);

    await waitFor(() => {
      expect(screen.getByTestId('block-editor')).toBeInTheDocument();
    });

    const input = screen.getByRole('textbox');
    await userEvent.click(input);

    // Simulate pressing Enter key
    const enterEvent = new KeyboardEvent('keydown', {
      key: 'Enter',
      bubbles: true,
      cancelable: true,
    });
    input.dispatchEvent(enterEvent);

    await waitFor(() => {
      const editors = screen.getAllByTestId('block-editor');
      expect(editors.length).toBeGreaterThan(1);
    });
  });

  it('should indent block when Tab is pressed', async () => {
    fakeBackend.database.addTask('First block', null);
    fakeBackend.database.addTask('Second block', null);

    render(<OutlinerTree />);

    await waitFor(() => {
      expect(screen.getAllByTestId('block-editor')).toHaveLength(2);
    });

    const inputs = screen.getAllByRole('textbox');
    await userEvent.click(inputs[1]);

    // Simulate pressing Tab key using fireEvent to trigger React's onKeyDown
    fireEvent.keyDown(inputs[1], { key: 'Tab', code: 'Tab' });

    await waitFor(() => {
      // Verify the block was moved
      const tasks = fakeBackend.database.getTasks();
      const secondBlock = tasks.find(t => t.id === '2');
      expect(secondBlock?.parent_id).toBe('1');
    }, { timeout: 2000 });
  });

  it.skip('should not show hover background when block is being edited', async () => {
    fakeBackend.database.addTask('Test block', null);

    const { container } = render(<OutlinerTree />);

    await waitFor(() => {
      expect(screen.getByTestId('block-editor')).toBeInTheDocument();
    });

    const input = screen.getByRole('textbox');

    // Click on the input to focus it
    await userEvent.click(input);

    await waitFor(() => {
      const blockDiv = container.querySelector('.ls-block');
      expect(blockDiv).toHaveClass('editing');
    }, { timeout: 1000 });
  });
});
