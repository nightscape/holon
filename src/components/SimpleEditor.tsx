import React from 'react';
import { useEditor, EditorContent } from '@tiptap/react';
import StarterKit from '@tiptap/starter-kit';

export const SimpleEditor: React.FC = () => {
  const editor = useEditor({
    extensions: [StarterKit],
    content: '<p>Type here with spaces...</p>',
    editorProps: {
      attributes: {
        style: 'border: 1px solid #ccc; padding: 10px; min-height: 100px;',
      },
    },
  });

  return (
    <div style={{ padding: '20px' }}>
      <h3>Simple Test Editor</h3>
      <EditorContent editor={editor} />
      {editor && (
        <div style={{ marginTop: '10px', padding: '10px', background: '#f0f0f0' }}>
          <strong>Content:</strong> {editor.getText()}
        </div>
      )}
    </div>
  );
};
