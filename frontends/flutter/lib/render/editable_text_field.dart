import 'package:flutter/material.dart';
import 'package:flutter/services.dart';
import 'package:flutter_hooks/flutter_hooks.dart';
import 'package:hooks_riverpod/hooks_riverpod.dart';
import '../providers/settings_provider.dart';

/// Hook-based widget wrapper for editable text field with Enter key handling.
///
/// Handles Enter key to save (without Shift) vs Shift+Enter for newlines.
class EditableTextField extends HookConsumerWidget {
  final String text;
  final void Function(String)? onSave;

  const EditableTextField({required this.text, this.onSave, super.key});

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
    final isShiftPressed = useRef<bool>(false);

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

    // Wrap in Flexible to provide bounded width constraints when used in Row
    final textField = Focus(
      onKeyEvent: (node, event) {
        // Track Shift key state locally
        if (event is KeyDownEvent) {
          if (event.logicalKey == LogicalKeyboardKey.shiftLeft ||
              event.logicalKey == LogicalKeyboardKey.shiftRight) {
            isShiftPressed.value = true;
          }
        } else if (event is KeyUpEvent) {
          if (event.logicalKey == LogicalKeyboardKey.shiftLeft ||
              event.logicalKey == LogicalKeyboardKey.shiftRight) {
            isShiftPressed.value = false;
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
          // Handle Enter key (without Shift) to save
          onEditingComplete: onSave != null
              ? () {
                  if (!isShiftPressed.value) {
                    _saveAndUnfocus(focusNode, controller);
                  }
                }
              : null,
        ),
      ),
    );

    return Flexible(child: textField);
  }
}
