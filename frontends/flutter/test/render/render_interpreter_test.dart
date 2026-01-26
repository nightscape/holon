import 'package:flutter/material.dart';
import 'package:flutter_test/flutter_test.dart';
import 'package:holon/render/render_interpreter.dart';
import 'package:holon/src/rust/third_party/query_render/types.dart';

void main() {
  group('RenderInterpreter', () {
    late RenderInterpreter interpreter;
    late RenderContext context;

    setUp(() {
      interpreter = RenderInterpreter();
      context = RenderContext(
        rowData: {
          'id': 'block-123',
          'content': 'Hello World',
          'depth': 2,
          'is_collapsed': false,
          'completed': true,
        },
      );
    });

    group('Literal Rendering', () {
      testWidgets('renders string literal', (tester) async {
        final expr = RenderExpr.literal(value: Value.string('Test String'));

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('Test String'), findsOneWidget);
      });

      testWidgets('renders integer literal', (tester) async {
        final expr = RenderExpr.literal(value: Value.number(Number.int(42)));

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('42'), findsOneWidget);
      });

      testWidgets('renders float literal', (tester) async {
        final expr = RenderExpr.literal(
          value: Value.number(Number.float(3.14)),
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('3.14'), findsOneWidget);
      });

      testWidgets('renders boolean literal', (tester) async {
        final expr = RenderExpr.literal(value: Value.bool(true));

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('true'), findsOneWidget);
      });

      testWidgets('renders null literal', (tester) async {
        final expr = RenderExpr.literal(value: Value.null_());

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('null'), findsOneWidget);
      });
    });

    group('Column Reference Rendering', () {
      testWidgets('renders column reference', (tester) async {
        final expr = RenderExpr.columnRef(name: 'content');

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('Hello World'), findsOneWidget);
      });

      testWidgets('renders empty string for missing column', (tester) async {
        final expr = RenderExpr.columnRef(name: 'nonexistent');

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text(''), findsOneWidget);
      });
    });

    group('Binary Operations', () {
      testWidgets('evaluates arithmetic: depth * 24', (tester) async {
        final expr = RenderExpr.binaryOp(
          op: BinaryOperator.mul,
          left: RenderExpr.columnRef(name: 'depth'),
          right: RenderExpr.literal(value: Value.number(Number.int(24))),
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('48'), findsOneWidget);
      });

      testWidgets('evaluates comparison: depth > 1', (tester) async {
        final expr = RenderExpr.binaryOp(
          op: BinaryOperator.gt,
          left: RenderExpr.columnRef(name: 'depth'),
          right: RenderExpr.literal(value: Value.number(Number.int(1))),
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('true'), findsOneWidget);
      });

      testWidgets('evaluates logical: completed and visible', (tester) async {
        final contextWithVisible = RenderContext(
          rowData: {...context.rowData, 'visible': true},
        );

        final expr = RenderExpr.binaryOp(
          op: BinaryOperator.and,
          left: RenderExpr.columnRef(name: 'completed'),
          right: RenderExpr.columnRef(name: 'visible'),
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, contextWithVisible)),
        );

        expect(find.text('true'), findsOneWidget);
      });
    });

    group('Function Call: text()', () {
      testWidgets('renders text with positional argument', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'text',
          args: [
            Arg(
              name: null,
              value: RenderExpr.literal(value: Value.string('Test Text')),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('Test Text'), findsOneWidget);
      });

      testWidgets('renders text with named argument', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'text',
          args: [
            Arg(
              name: 'value',
              value: RenderExpr.literal(value: Value.string('Named Text')),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('Named Text'), findsOneWidget);
      });

      testWidgets('renders text with column reference', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'text',
          args: [Arg(name: null, value: RenderExpr.columnRef(name: 'content'))],
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('Hello World'), findsOneWidget);
      });
    });

    group('Function Call: block()', () {
      testWidgets('renders block with children', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'block',
          args: [
            Arg(
              name: null,
              value: RenderExpr.functionCall(
                name: 'text',
                args: [
                  Arg(
                    name: null,
                    value: RenderExpr.literal(value: Value.string('Child 1')),
                  ),
                ],
              ),
            ),
            Arg(
              name: null,
              value: RenderExpr.functionCall(
                name: 'text',
                args: [
                  Arg(
                    name: null,
                    value: RenderExpr.literal(value: Value.string('Child 2')),
                  ),
                ],
              ),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: Scaffold(body: interpreter.build(expr, context))),
        );

        expect(find.text('Child 1'), findsOneWidget);
        expect(find.text('Child 2'), findsOneWidget);
      });

      testWidgets('applies indentation based on depth', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'block',
          args: [
            Arg(
              name: 'depth',
              value: RenderExpr.columnRef(name: 'depth'),
            ),
            Arg(
              name: null,
              value: RenderExpr.functionCall(
                name: 'text',
                args: [
                  Arg(
                    name: null,
                    value: RenderExpr.literal(value: Value.string('Indented')),
                  ),
                ],
              ),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: Scaffold(body: interpreter.build(expr, context))),
        );

        // Find the Padding widget
        final padding = tester.widget<Padding>(find.byType(Padding));
        expect(
          padding.padding,
          equals(const EdgeInsets.only(left: 48.0)),
        ); // 2 * 24
      });
    });

    group('Function Call: row()', () {
      testWidgets('renders row with multiple children', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'row',
          args: [
            Arg(
              name: null,
              value: RenderExpr.functionCall(
                name: 'text',
                args: [
                  Arg(
                    name: null,
                    value: RenderExpr.literal(value: Value.string('Item 1')),
                  ),
                ],
              ),
            ),
            Arg(
              name: null,
              value: RenderExpr.functionCall(
                name: 'text',
                args: [
                  Arg(
                    name: null,
                    value: RenderExpr.literal(value: Value.string('Item 2')),
                  ),
                ],
              ),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.text('Item 1'), findsOneWidget);
        expect(find.text('Item 2'), findsOneWidget);
        expect(find.byType(Row), findsOneWidget);
      });
    });

    group('Function Call: editable_text()', () {
      testWidgets('renders editable text with content', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'editable_text',
          args: [
            Arg(
              name: 'content',
              value: RenderExpr.columnRef(name: 'content'),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: Scaffold(body: interpreter.build(expr, context))),
        );

        expect(find.byType(TextField), findsOneWidget);
        final textField = tester.widget<TextField>(find.byType(TextField));
        expect(textField.controller?.text, equals('Hello World'));
      });
    });

    group('Function Call: collapse_button()', () {
      testWidgets('renders collapsed button', (tester) async {
        final collapsedContext = RenderContext(
          rowData: {...context.rowData, 'is_collapsed': true},
        );

        final expr = RenderExpr.functionCall(
          name: 'collapse_button',
          args: [
            Arg(
              name: 'is_collapsed',
              value: RenderExpr.columnRef(name: 'is_collapsed'),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, collapsedContext)),
        );

        expect(find.byIcon(Icons.chevron_right), findsOneWidget);
      });

      testWidgets('renders expanded button', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'collapse_button',
          args: [
            Arg(
              name: 'is_collapsed',
              value: RenderExpr.columnRef(name: 'is_collapsed'),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.byIcon(Icons.expand_more), findsOneWidget);
      });
    });

    group('Function Call: drop_zone()', () {
      testWidgets('renders drop zone', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'drop_zone',
          args: [
            Arg(
              name: 'position',
              value: RenderExpr.literal(value: Value.string('before')),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(find.byType(Container), findsWidgets);
      });
    });

    group('Array Rendering', () {
      testWidgets('renders array as column', (tester) async {
        final expr = RenderExpr.array(
          items: [
            RenderExpr.functionCall(
              name: 'text',
              args: [
                Arg(
                  name: null,
                  value: RenderExpr.literal(value: Value.string('Item 1')),
                ),
              ],
            ),
            RenderExpr.functionCall(
              name: 'text',
              args: [
                Arg(
                  name: null,
                  value: RenderExpr.literal(value: Value.string('Item 2')),
                ),
              ],
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: Scaffold(body: interpreter.build(expr, context))),
        );

        expect(find.text('Item 1'), findsOneWidget);
        expect(find.text('Item 2'), findsOneWidget);
        expect(find.byType(Column), findsOneWidget);
      });
    });

    group('Unknown Function Handling', () {
      testWidgets('shows error for unknown function', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'unknown_function',
          args: [],
        );

        await tester.pumpWidget(
          MaterialApp(home: interpreter.build(expr, context)),
        );

        expect(
          find.textContaining('Unknown function: unknown_function'),
          findsOneWidget,
        );
      });
    });

    group('Complex Expressions', () {
      testWidgets('renders nested block with operations', (tester) async {
        final expr = RenderExpr.functionCall(
          name: 'block',
          args: [
            Arg(
              name: 'depth',
              value: RenderExpr.columnRef(name: 'depth'),
            ),
            Arg(
              name: null,
              value: RenderExpr.functionCall(
                name: 'row',
                args: [
                  Arg(
                    name: null,
                    value: RenderExpr.functionCall(
                      name: 'collapse_button',
                      args: [
                        Arg(
                          name: 'is_collapsed',
                          value: RenderExpr.columnRef(name: 'is_collapsed'),
                        ),
                      ],
                    ),
                  ),
                  Arg(
                    name: null,
                    value: RenderExpr.functionCall(
                      name: 'flexible',
                      args: [
                        Arg(
                          name: null,
                          value: RenderExpr.functionCall(
                            name: 'editable_text',
                            args: [
                              Arg(
                                name: 'content',
                                value: RenderExpr.columnRef(name: 'content'),
                              ),
                            ],
                          ),
                        ),
                      ],
                    ),
                  ),
                ],
              ),
            ),
          ],
        );

        await tester.pumpWidget(
          MaterialApp(home: Scaffold(body: interpreter.build(expr, context))),
        );

        // Check for block-level Padding (with indentation)
        final paddingWidgets = tester
            .widgetList<Padding>(find.byType(Padding))
            .toList();
        final blockPadding = paddingWidgets.where(
          (p) => p.padding == const EdgeInsets.only(left: 48.0),
        );
        expect(blockPadding, hasLength(1));

        expect(find.byType(Column), findsOneWidget);
        expect(find.byType(Row), findsOneWidget);
        expect(find.byIcon(Icons.expand_more), findsOneWidget);
        expect(find.byType(TextField), findsOneWidget);
      });
    });

    group('RenderContext', () {
      test('getColumn returns value', () {
        expect(context.getColumn('content'), equals('Hello World'));
      });

      test('getColumn returns null for missing column', () {
        expect(context.getColumn('nonexistent'), isNull);
      });

      test('getTypedColumn returns typed value', () {
        expect(
          context.getTypedColumn<String>('content'),
          equals('Hello World'),
        );
        expect(context.getTypedColumn<int>('depth'), equals(2));
      });

      test('getTypedColumn throws on type mismatch', () {
        expect(
          () => context.getTypedColumn<int>('content'),
          throwsArgumentError,
        );
      });
    });
  });
}
