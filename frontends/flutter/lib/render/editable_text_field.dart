import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_hooks/flutter_hooks.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import '../providers/settings_provider.dart';

/// Hook-based widget wrapper for editable text field with Enter key handling.
///
/// Handles Enter key to split block (without Shift) vs Shift+Enter for newlines.
/// If onSplit is provided, Enter (without Shift) will call onSplit with cursor position.
/// If onSplit is null, Enter (without Shift) will save and unfocus.
class EditableTextField extends HookConsumerWidget {
  final String text;
  final void Function(String)? onSave;
  final Future<void> Function(int cursorPosition)? onSplit;

  const EditableTextField({
    required this.text,
    this.onSave,
    this.onSplit,
    super.key,
  });

  void _saveAndUnfocus(FocusNode focusNode, TextEditingController controller) {
    if (onSave != null) {
      onSave!(controller.text);
    }
    focusNode.unfocus();
  }

  @override
  Widget build(BuildContext context, WidgetRef ref) {
    final colors = ref.watch(appColorsProvider);
    final controller = useTextEditingController(text: text);
    final focusNode = useFocusNode();
    final wasFocused = useRef<bool>(focusNode.hasFocus);

    // Sync controller text with prop when prop changes and we are not editing
    // or if we want to force update from external source.
    // Note: If we update while focused, we might disrupt typing.
    // But if we don't, we show stale data.
    // Given the requirement "changes can originate externally", we should update.
    // To avoid cursor jumping, we can try to preserve selection,
    // but if text is different, selection might be invalid.
    // For now, we update if text is different.
    useEffect(() {
      if (controller.text != text) {
        // Preserve selection if possible
        final selection = controller.selection;
        controller.text = text;
        if (selection.isValid && selection.end <= text.length) {
          controller.selection = selection;
        }
      }
      return null;
    }, [text]);

    // Listen for focus changes to save when focus is lost (backup listener)
    // Note: The Focus widget's onFocusChange callback is the primary handler
    useEffect(() {
      void listener() {
        final isFocused = focusNode.hasFocus;

        // Save when transitioning from focused to unfocused
        if (wasFocused.value && !isFocused && onSave != null) {
          onSave!(controller.text);
        }
        wasFocused.value = isFocused;
      }

      focusNode.addListener(listener);
      return () => focusNode.removeListener(listener);
    }, [focusNode, onSave, controller]);

    // Use Focus widget to intercept Enter key before TextField processes it
    // This matches the approach used in outliner-flutter
    final textField = Focus(
      onKeyEvent: (node, event) {
        // DEBUG: Set breakpoint here to see if onKeyEvent is being called
        print(
          '[EditableTextField] onKeyEvent: ${event.runtimeType}, key: ${event.logicalKey}, shift: ${HardwareKeyboard.instance.isShiftPressed}',
        );

        if (event is KeyDownEvent) {
          // Handle Enter key (without Shift) to split block
          if (event.logicalKey == LogicalKeyboardKey.enter &&
              !HardwareKeyboard.instance.isShiftPressed &&
              onSplit != null) {
            print(
              '[EditableTextField] Enter pressed without Shift, onSplit available - calling split',
            );
            final cursorPosition = controller.selection.baseOffset;
            // Save content first (if onSave is provided) before splitting,
            // because split_block reads content from the entity, not the TextField controller
            if (onSave != null && controller.text != text) {
              print(
                '[EditableTextField] Saving content before split: "${controller.text}"',
              );
              onSave!(controller.text);
            }
            // Call onSplit asynchronously (it handles saving internally if needed)
            print(
              '[EditableTextField] Calling onSplit with cursor position: $cursorPosition',
            );
            onSplit!(cursorPosition);
            // Return handled to prevent TextField from processing the key
            print('[EditableTextField] Returning KeyEventResult.handled');
            return KeyEventResult.handled;
          } else if (event.logicalKey == LogicalKeyboardKey.enter) {
            print(
              '[EditableTextField] Enter pressed but conditions not met: shift=${HardwareKeyboard.instance.isShiftPressed}, onSplit=${onSplit != null}',
            );
          }
        }
        return KeyEventResult.ignored;
      },
      child: Actions(
        actions: {
          // Disable focus traversal for Tab key
          NextFocusIntent: DoNothingAction(consumesKey: false),
          PreviousFocusIntent: DoNothingAction(consumesKey: false),
        },
        child: TextField(
          controller: controller,
          focusNode: focusNode,
          decoration: const InputDecoration(
            border: InputBorder.none,
            enabledBorder: InputBorder.none,
            focusedBorder: InputBorder.none,
            isDense: true,
            contentPadding: EdgeInsets.zero,
          ),
          style: TextStyle(
            fontSize: 16,
            height: 1.5,
            color: colors.textPrimary,
            letterSpacing: 0,
          ),
          maxLines: null,
          minLines: 1,
          textInputAction: TextInputAction.newline,
          // Don't set onEditingComplete when onSplit is provided - let Focus widget handle Enter
          // Only use onEditingComplete for save/unfocus when onSplit is not available
          onEditingComplete: onSave != null && onSplit == null
              ? () {
                  if (!HardwareKeyboard.instance.isShiftPressed) {
                    _saveAndUnfocus(focusNode, controller);
                  }
                }
              : null,
        ),
      ),
    );

    return textField;
  }
}
