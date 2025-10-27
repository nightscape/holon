import React, { useEffect, useRef, useCallback } from 'react';
import TextareaAutosize from 'react-textarea-autosize';

interface BlockEditorProps {
  content: string;
  onUpdate: (content: string) => void;
  onEnter?: () => void;
  onBackspace?: () => void;
  onTab?: () => void;
  onFocusChange?: (focused: boolean) => void;
  autoFocus?: boolean;
}

export const BlockEditor: React.FC<BlockEditorProps> = ({
  content,
  onUpdate,
  onEnter,
  onBackspace,
  onTab,
  onFocusChange,
  autoFocus = false,
}) => {
  const textareaRef = useRef<HTMLTextAreaElement>(null);
  const isComposingRef = useRef(false);

  useEffect(() => {
    if (autoFocus && textareaRef.current) {
      textareaRef.current.focus();
      const length = textareaRef.current.value.length;
      textareaRef.current.setSelectionRange(length, length);
    }
  }, [autoFocus]);

  const handleChange = useCallback(
    (e: React.ChangeEvent<HTMLTextAreaElement>) => {
      onUpdate(e.target.value);
    },
    [onUpdate]
  );

  const handleCompositionStart = useCallback(() => {
    isComposingRef.current = true;
  }, []);

  const handleCompositionUpdate = useCallback(() => {
    isComposingRef.current = true;
  }, []);

  const handleCompositionEnd = useCallback(
    (e: React.CompositionEvent<HTMLTextAreaElement>) => {
      isComposingRef.current = false;
      onUpdate((e.target as HTMLTextAreaElement).value);
    },
    [onUpdate]
  );

  const handleKeyDown = useCallback(
    (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
      console.log('[BlockEditor] Key down:', e.key, 'isComposing:', isComposingRef.current);

      if (e.key === 'Tab' && !e.shiftKey) {
        console.log('[BlockEditor] Tab key pressed - calling onTab');
        e.preventDefault();
        e.stopPropagation();
        onTab?.();
        return;
      }

      if (isComposingRef.current) {
        console.log('[BlockEditor] Skipping - composition in progress');
        return;
      }

      if (e.key === 'Enter' && !e.shiftKey) {
        console.log('[BlockEditor] Enter key pressed - calling onEnter');
        e.preventDefault();
        e.stopPropagation();
        onEnter?.();
        return;
      }

      if (e.key === 'Backspace' && e.currentTarget.value === '') {
        console.log('[BlockEditor] Backspace on empty - calling onBackspace');
        e.preventDefault();
        e.stopPropagation();
        onBackspace?.();
        return;
      }
    },
    [onEnter, onBackspace, onTab]
  );

  const handleFocus = useCallback(() => {
    console.log('[BlockEditor] Editor focused');
    onFocusChange?.(true);
  }, [onFocusChange]);

  const handleBlur = useCallback(() => {
    console.log('[BlockEditor] Editor blurred');
    onFocusChange?.(false);
  }, [onFocusChange]);

  return (
    <TextareaAutosize
      ref={textareaRef}
      value={content}
      onChange={handleChange}
      onCompositionStart={handleCompositionStart}
      onCompositionUpdate={handleCompositionUpdate}
      onCompositionEnd={handleCompositionEnd}
      onKeyDown={handleKeyDown}
      onFocus={handleFocus}
      onBlur={handleBlur}
      className="block-content focus:outline-none w-full resize-none bg-transparent"
      style={{ color: 'var(--ls-primary-text-color)' }}
      data-testid="block editor"
      minRows={1}
      maxRows={20}
      tabIndex={0}
    />
  );
};
