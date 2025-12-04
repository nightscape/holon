import 'package:flutter/material.dart';
import '../src/rust/third_party/holon_api/block.dart';
import '../src/rust/third_party/holon_api.dart';
import '../styles/app_styles.dart';

/// Widget for displaying a source code block with optional editing and results.
///
/// Supports PRQL and other languages with syntax highlighting (basic).
/// Part of Phase 7: Flutter Integration for Block Serialization.
class SourceBlockWidget extends StatefulWidget {
  final String language;
  final String source;
  final String? name;
  final BlockResult? results;
  final bool editable;
  final void Function(String)? onSourceChanged;
  final Future<void> Function()? onExecute;
  final AppColors colors;

  const SourceBlockWidget({
    super.key,
    required this.language,
    required this.source,
    this.name,
    this.results,
    this.editable = false,
    this.onSourceChanged,
    this.onExecute,
    required this.colors,
  });

  @override
  State<SourceBlockWidget> createState() => _SourceBlockWidgetState();
}

class _SourceBlockWidgetState extends State<SourceBlockWidget> {
  late TextEditingController _controller;
  bool _isExecuting = false;
  bool _isExpanded = true;

  @override
  void initState() {
    super.initState();
    _controller = TextEditingController(text: widget.source);
  }

  @override
  void didUpdateWidget(SourceBlockWidget oldWidget) {
    super.didUpdateWidget(oldWidget);
    if (oldWidget.source != widget.source) {
      _controller.text = widget.source;
    }
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  Future<void> _handleExecute() async {
    if (widget.onExecute == null || _isExecuting) return;

    setState(() => _isExecuting = true);
    try {
      await widget.onExecute!();
    } finally {
      if (mounted) {
        setState(() => _isExecuting = false);
      }
    }
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      margin: const EdgeInsets.symmetric(vertical: 8),
      decoration: BoxDecoration(
        color: widget.colors.backgroundSecondary,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: widget.colors.border),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        mainAxisSize: MainAxisSize.min,
        children: [
          // Header with language badge and controls
          _buildHeader(),
          // Source code content
          if (_isExpanded) ...[
            Container(height: 1, color: widget.colors.border),
            _buildSourceContent(),
          ],
          // Results section (if available)
          if (_isExpanded && widget.results != null) ...[
            Container(height: 1, color: widget.colors.border),
            QueryResultWidget(result: widget.results!, colors: widget.colors),
          ],
        ],
      ),
    );
  }

  Widget _buildHeader() {
    return Padding(
      padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 8),
      child: Row(
        children: [
          // Expand/collapse button
          GestureDetector(
            onTap: () => setState(() => _isExpanded = !_isExpanded),
            child: Icon(
              _isExpanded ? Icons.expand_more : Icons.chevron_right,
              size: 18,
              color: widget.colors.textTertiary,
            ),
          ),
          const SizedBox(width: 8),
          // Language badge
          _LanguageBadge(language: widget.language, colors: widget.colors),
          const SizedBox(width: 8),
          // Optional name
          if (widget.name != null) ...[
            Text(
              widget.name!,
              style: TextStyle(
                fontSize: 12,
                fontWeight: FontWeight.w500,
                color: widget.colors.textSecondary,
              ),
            ),
            const SizedBox(width: 8),
          ],
          const Spacer(),
          // Execute button
          if (widget.onExecute != null)
            _IconButton(
              icon: _isExecuting ? Icons.hourglass_empty : Icons.play_arrow,
              tooltip: 'Execute',
              onPressed: _isExecuting ? null : _handleExecute,
              colors: widget.colors,
            ),
          // Copy button
          _IconButton(
            icon: Icons.copy,
            tooltip: 'Copy',
            onPressed: () {
              // TODO: Copy to clipboard
            },
            colors: widget.colors,
          ),
        ],
      ),
    );
  }

  Widget _buildSourceContent() {
    if (widget.editable) {
      return Padding(
        padding: const EdgeInsets.all(12),
        child: TextField(
          controller: _controller,
          maxLines: null,
          style: _codeStyle(widget.colors),
          decoration: const InputDecoration(
            border: InputBorder.none,
            isDense: true,
            contentPadding: EdgeInsets.zero,
          ),
          onChanged: widget.onSourceChanged,
        ),
      );
    }

    return Padding(
      padding: const EdgeInsets.all(12),
      child: _SyntaxHighlightedCode(
        source: widget.source,
        language: widget.language,
        colors: widget.colors,
      ),
    );
  }
}

/// Simple syntax-highlighted code display.
/// For now, provides basic keyword highlighting for PRQL.
class _SyntaxHighlightedCode extends StatelessWidget {
  final String source;
  final String language;
  final AppColors colors;

  const _SyntaxHighlightedCode({
    required this.source,
    required this.language,
    required this.colors,
  });

  @override
  Widget build(BuildContext context) {
    if (language.toLowerCase() == 'prql') {
      return _buildPrqlHighlighted();
    }
    // Default: plain text
    return SelectableText(source, style: _codeStyle(colors));
  }

  Widget _buildPrqlHighlighted() {
    // PRQL keywords for basic highlighting
    const keywords = [
      'from',
      'select',
      'filter',
      'derive',
      'group',
      'aggregate',
      'sort',
      'take',
      'join',
      'window',
      'let',
      'func',
      'prql',
      'case',
      'null',
      'true',
      'false',
    ];
    const operators = [
      '==',
      '!=',
      '>=',
      '<=',
      '>',
      '<',
      '&&',
      '||',
      '+',
      '-',
      '*',
      '/',
    ];
    const builtins = ['sum', 'count', 'avg', 'min', 'max', 'first', 'last'];

    final spans = <TextSpan>[];
    final lines = source.split('\n');

    for (var i = 0; i < lines.length; i++) {
      if (i > 0) spans.add(const TextSpan(text: '\n'));

      final line = lines[i];
      var pos = 0;

      while (pos < line.length) {
        // Check for comment
        if (line.substring(pos).startsWith('#')) {
          spans.add(
            TextSpan(
              text: line.substring(pos),
              style: TextStyle(
                color: colors.textTertiary,
                fontStyle: FontStyle.italic,
              ),
            ),
          );
          break;
        }

        // Check for string literal
        if (line[pos] == '"' || line[pos] == "'") {
          final quote = line[pos];
          var end = pos + 1;
          while (end < line.length && line[end] != quote) {
            if (line[end] == '\\' && end + 1 < line.length) end++;
            end++;
          }
          if (end < line.length) end++;
          spans.add(
            TextSpan(
              text: line.substring(pos, end),
              style: TextStyle(color: colors.success),
            ),
          );
          pos = end;
          continue;
        }

        // Check for number
        if (RegExp(r'^\d').hasMatch(line.substring(pos))) {
          final match = RegExp(r'^\d+\.?\d*').firstMatch(line.substring(pos));
          if (match != null) {
            spans.add(
              TextSpan(
                text: match.group(0),
                style: TextStyle(color: colors.warning),
              ),
            );
            pos += match.group(0)!.length;
            continue;
          }
        }

        // Check for keyword/identifier
        final wordMatch = RegExp(
          r'^[a-zA-Z_]\w*',
        ).firstMatch(line.substring(pos));
        if (wordMatch != null) {
          final word = wordMatch.group(0)!;
          Color? wordColor;

          if (keywords.contains(word.toLowerCase())) {
            wordColor = colors.primary;
          } else if (builtins.contains(word.toLowerCase())) {
            wordColor = colors.warning;
          }

          spans.add(
            TextSpan(
              text: word,
              style: wordColor != null
                  ? TextStyle(color: wordColor, fontWeight: FontWeight.w600)
                  : _codeStyle(colors),
            ),
          );
          pos += word.length;
          continue;
        }

        // Check for operator
        var foundOp = false;
        for (final op in operators) {
          if (line.substring(pos).startsWith(op)) {
            spans.add(
              TextSpan(
                text: op,
                style: TextStyle(color: colors.error),
              ),
            );
            pos += op.length;
            foundOp = true;
            break;
          }
        }
        if (foundOp) continue;

        // Default: single character
        spans.add(TextSpan(text: line[pos], style: _codeStyle(colors)));
        pos++;
      }
    }

    return SelectableText.rich(TextSpan(children: spans));
  }
}

/// Language badge widget
class _LanguageBadge extends StatelessWidget {
  final String language;
  final AppColors colors;

  const _LanguageBadge({required this.language, required this.colors});

  @override
  Widget build(BuildContext context) {
    final isPrql = language.toLowerCase() == 'prql';

    return Container(
      padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
      decoration: BoxDecoration(
        color: isPrql
            ? colors.primary.withValues(alpha: 0.15)
            : colors.textTertiary.withValues(alpha: 0.15),
        borderRadius: BorderRadius.circular(4),
      ),
      child: Text(
        language.toUpperCase(),
        style: TextStyle(
          fontSize: 10,
          fontWeight: FontWeight.w600,
          letterSpacing: 0.5,
          color: isPrql ? colors.primary : colors.textSecondary,
        ),
      ),
    );
  }
}

/// Small icon button for source block header
class _IconButton extends StatelessWidget {
  final IconData icon;
  final String tooltip;
  final VoidCallback? onPressed;
  final AppColors colors;

  const _IconButton({
    required this.icon,
    required this.tooltip,
    this.onPressed,
    required this.colors,
  });

  @override
  Widget build(BuildContext context) {
    return Tooltip(
      message: tooltip,
      child: InkWell(
        onTap: onPressed,
        borderRadius: BorderRadius.circular(4),
        child: Padding(
          padding: const EdgeInsets.all(4),
          child: Icon(
            icon,
            size: 16,
            color: onPressed != null
                ? colors.textSecondary
                : colors.textTertiary,
          ),
        ),
      ),
    );
  }
}

/// Widget for editing source code with a simple text field.
class SourceEditorWidget extends StatefulWidget {
  final String language;
  final String initialContent;
  final void Function(String)? onChanged;
  final AppColors colors;

  const SourceEditorWidget({
    super.key,
    required this.language,
    required this.initialContent,
    this.onChanged,
    required this.colors,
  });

  @override
  State<SourceEditorWidget> createState() => _SourceEditorWidgetState();
}

class _SourceEditorWidgetState extends State<SourceEditorWidget> {
  late TextEditingController _controller;

  @override
  void initState() {
    super.initState();
    _controller = TextEditingController(text: widget.initialContent);
  }

  @override
  void dispose() {
    _controller.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return Container(
      decoration: BoxDecoration(
        color: widget.colors.backgroundSecondary,
        borderRadius: BorderRadius.circular(8),
        border: Border.all(color: widget.colors.border),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.stretch,
        mainAxisSize: MainAxisSize.min,
        children: [
          // Language badge header
          Padding(
            padding: const EdgeInsets.all(8),
            child: _LanguageBadge(
              language: widget.language,
              colors: widget.colors,
            ),
          ),
          Container(height: 1, color: widget.colors.border),
          // Editor
          Padding(
            padding: const EdgeInsets.all(12),
            child: TextField(
              controller: _controller,
              maxLines: null,
              minLines: 3,
              style: _codeStyle(widget.colors),
              decoration: const InputDecoration(
                border: InputBorder.none,
                isDense: true,
                contentPadding: EdgeInsets.zero,
                hintText: 'Enter source code...',
              ),
              onChanged: widget.onChanged,
            ),
          ),
        ],
      ),
    );
  }
}

/// Widget for displaying query execution results.
///
/// Handles three output types:
/// - Text: Plain text output
/// - Table: Tabular data with headers and rows
/// - Error: Error message display
class QueryResultWidget extends StatelessWidget {
  final BlockResult result;
  final AppColors colors;

  const QueryResultWidget({
    super.key,
    required this.result,
    required this.colors,
  });

  @override
  Widget build(BuildContext context) {
    return Container(
      padding: const EdgeInsets.all(12),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        mainAxisSize: MainAxisSize.min,
        children: [
          // Result header with timestamp
          Row(
            children: [
              Icon(Icons.output, size: 14, color: colors.textTertiary),
              const SizedBox(width: 4),
              Text(
                'Results',
                style: TextStyle(
                  fontSize: 11,
                  fontWeight: FontWeight.w600,
                  color: colors.textTertiary,
                ),
              ),
              const Spacer(),
              Text(
                _formatTimestamp(result.executedAt.toInt()),
                style: TextStyle(fontSize: 10, color: colors.textTertiary),
              ),
            ],
          ),
          const SizedBox(height: 8),
          // Result content
          _buildResultContent(),
        ],
      ),
    );
  }

  Widget _buildResultContent() {
    return switch (result.output) {
      ResultOutput_Text(:final content) => _buildTextResult(content),
      ResultOutput_Table(:final headers, :final rows) => _buildTableResult(
        headers,
        rows,
      ),
      ResultOutput_Error(:final message) => _buildErrorResult(message),
    };
  }

  Widget _buildTextResult(String content) {
    return SelectableText(content, style: _codeStyle(colors));
  }

  Widget _buildTableResult(List<String> headers, List<List<Value>> rows) {
    if (headers.isEmpty && rows.isEmpty) {
      return Text(
        'No results',
        style: TextStyle(
          fontSize: 12,
          fontStyle: FontStyle.italic,
          color: colors.textTertiary,
        ),
      );
    }

    return SingleChildScrollView(
      scrollDirection: Axis.horizontal,
      child: DataTable(
        headingRowHeight: 32,
        dataRowMinHeight: 28,
        dataRowMaxHeight: 28,
        columnSpacing: 24,
        horizontalMargin: 8,
        headingTextStyle: TextStyle(
          fontSize: 11,
          fontWeight: FontWeight.w600,
          color: colors.textSecondary,
        ),
        dataTextStyle: TextStyle(fontSize: 12, color: colors.textPrimary),
        columns: headers.map((h) => DataColumn(label: Text(h))).toList(),
        rows: rows.map((row) {
          return DataRow(
            cells: row.map((cell) {
              return DataCell(Text(_valueToString(cell)));
            }).toList(),
          );
        }).toList(),
      ),
    );
  }

  Widget _buildErrorResult(String message) {
    return Container(
      padding: const EdgeInsets.all(8),
      decoration: BoxDecoration(
        color: colors.error.withValues(alpha: 0.1),
        borderRadius: BorderRadius.circular(4),
        border: Border.all(color: colors.error.withValues(alpha: 0.3)),
      ),
      child: Row(
        children: [
          Icon(Icons.error_outline, size: 16, color: colors.error),
          const SizedBox(width: 8),
          Expanded(
            child: SelectableText(
              message,
              style: TextStyle(
                fontSize: 12,
                color: colors.error,
                fontFamily: 'monospace',
              ),
            ),
          ),
        ],
      ),
    );
  }

  String _formatTimestamp(int timestampMs) {
    final dt = DateTime.fromMillisecondsSinceEpoch(timestampMs);
    final now = DateTime.now();
    final diff = now.difference(dt);

    if (diff.inSeconds < 60) {
      return 'just now';
    } else if (diff.inMinutes < 60) {
      return '${diff.inMinutes}m ago';
    } else if (diff.inHours < 24) {
      return '${diff.inHours}h ago';
    } else {
      return '${dt.month}/${dt.day} ${dt.hour}:${dt.minute.toString().padLeft(2, '0')}';
    }
  }

  String _valueToString(Value value) {
    // Use runtime type checking since freezed extension methods may not be in scope
    if (value is Value_String) return value.field0;
    if (value is Value_Integer) return value.field0.toString();
    if (value is Value_Float) return value.field0.toString();
    if (value is Value_Boolean) return value.field0.toString();
    if (value is Value_DateTime) return value.field0;
    if (value is Value_Json) return value.field0;
    if (value is Value_Reference) return value.field0;
    if (value is Value_Array) return '[${value.field0.length} items]';
    if (value is Value_Object) return '{${value.field0.length} fields}';
    if (value is Value_Null) return 'null';
    return value.toString();
  }
}

/// Code text style helper
TextStyle _codeStyle(AppColors colors) {
  return TextStyle(
    fontSize: 13,
    fontFamily: 'monospace',
    height: 1.5,
    color: colors.textPrimary,
  );
}
