import { describe, it, expect, vi } from 'vitest';
import { render, screen, waitFor, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { BlockEditor } from './BlockEditor';

describe('BlockEditor', () => {
  it('should render with initial content', () => {
    render(<BlockEditor content="Hello World" onUpdate={vi.fn()} />);

    const textarea = screen.getByTestId('block editor');
    expect(textarea).toHaveValue('Hello World');
  });

  it('should call onUpdate when content changes', async () => {
    const user = userEvent.setup();
    const onUpdate = vi.fn();
    render(<BlockEditor content="" onUpdate={onUpdate} />);

    const textarea = screen.getByTestId('block editor');
    await user.type(textarea, 'Hello');

    expect(onUpdate).toHaveBeenCalled();
    expect(onUpdate).toHaveBeenCalledWith('H');
  });

  it('should update content when prop changes', async () => {
    const { rerender } = render(
      <BlockEditor content="Initial" onUpdate={vi.fn()} />
    );

    let textarea = screen.getByTestId('block editor');
    expect(textarea).toHaveValue('Initial');

    rerender(<BlockEditor content="Updated" onUpdate={vi.fn()} />);

    await waitFor(() => {
      textarea = screen.getByTestId('block editor');
      expect(textarea).toHaveValue('Updated');
    });
  });

  it('should handle empty content', () => {
    render(<BlockEditor content="" onUpdate={vi.fn()} />);

    const textarea = screen.getByTestId('block editor');
    expect(textarea).toHaveValue('');
  });

  it('should handle plain text content', () => {
    render(<BlockEditor content="Bold and italic" onUpdate={vi.fn()} />);

    const textarea = screen.getByTestId('block editor');
    expect(textarea).toHaveValue('Bold and italic');
  });

  it('should render with block-content class', () => {
    render(<BlockEditor content="Test" onUpdate={vi.fn()} />);

    const textarea = screen.getByTestId('block editor');
    expect(textarea).toHaveClass('block-content');
  });

  it('should accept and use onEnter callback', () => {
    const onEnter = vi.fn();
    const { container } = render(<BlockEditor content="Test" onUpdate={vi.fn()} onEnter={onEnter} />);

    expect(container.querySelector('textarea')).toBeInTheDocument();
  });

  it('should accept and use onBackspace callback', () => {
    const onBackspace = vi.fn();
    const { container } = render(<BlockEditor content="" onUpdate={vi.fn()} onBackspace={onBackspace} />);

    expect(container.querySelector('textarea')).toBeInTheDocument();
  });

  it('should accept and use onTab callback', () => {
    const onTab = vi.fn();
    const { container } = render(<BlockEditor content="Test" onUpdate={vi.fn()} onTab={onTab} />);

    expect(container.querySelector('textarea')).toBeInTheDocument();
  });

  it('should call onFocusChange when focused', async () => {
    const user = userEvent.setup();
    const onFocusChange = vi.fn();
    render(<BlockEditor content="Test" onUpdate={vi.fn()} onFocusChange={onFocusChange} />);

    const textarea = screen.getByTestId('block editor');
    await user.click(textarea);

    expect(onFocusChange).toHaveBeenCalledWith(true);
  });

  it('should call onFocusChange when blurred', async () => {
    const user = userEvent.setup();
    const onFocusChange = vi.fn();
    render(
      <>
        <BlockEditor content="Test" onUpdate={vi.fn()} onFocusChange={onFocusChange} />
        <button>Outside</button>
      </>
    );

    const textarea = screen.getByTestId('block editor');
    await user.click(textarea);
    onFocusChange.mockClear();

    const button = screen.getByText('Outside');
    await user.click(button);

    expect(onFocusChange).toHaveBeenCalledWith(false);
  });

  it('should auto-focus when autoFocus is true', async () => {
    render(<BlockEditor content="Test" onUpdate={vi.fn()} autoFocus={true} />);

    await waitFor(() => {
      const textarea = screen.getByTestId('block editor');
      expect(textarea).toHaveFocus();
    });
  });

  it('should handle multiline content', () => {
    const multilineContent = 'Line 1\nLine 2\nLine 3';
    render(<BlockEditor content={multilineContent} onUpdate={vi.fn()} />);

    const textarea = screen.getByTestId('block editor');
    expect(textarea).toHaveValue(multilineContent);
  });

  it('should not update during composition', async () => {
    const onUpdate = vi.fn();
    render(<BlockEditor content="" onUpdate={onUpdate} />);

    const textarea = screen.getByTestId('block editor') as HTMLTextAreaElement;

    textarea.dispatchEvent(new CompositionEvent('compositionstart'));

    textarea.value = 'あ';
    textarea.dispatchEvent(new Event('change', { bubbles: true }));

    expect(onUpdate).not.toHaveBeenCalled();

    textarea.dispatchEvent(new CompositionEvent('compositionend', {
      data: 'あ',
      bubbles: true
    }));

    expect(onUpdate).toHaveBeenCalledWith('あ');
  });
});
