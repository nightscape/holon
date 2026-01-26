#!/usr/bin/env dart
// Script to collect Flutter/Dart coverage via VM Service Protocol
//
// Usage:
//   dart scripts/collect-flutter-coverage-vm-service.dart [vm-service-uri]
//
// Example:
//   dart scripts/collect-flutter-coverage-vm-service.dart http://localhost:8181
//
// Prerequisites:
//   - Add vm_service to dev_dependencies in pubspec.yaml

import 'dart:io';
import 'dart:convert';

/// Collect coverage data from VM Service
///
/// This script connects to the VM Service and collects coverage data
/// from the running Flutter application.
Future<void> main(List<String> args) async {
  final vmServiceUri = args.isNotEmpty ? args[0] : 'http://localhost:8181';

  print('🔍 Connecting to VM Service at: $vmServiceUri');

  try {
    // Connect to VM Service
    final client = await HttpClient().getUrl(Uri.parse('$vmServiceUri/vm'));
    final response = await client.close();

    if (response.statusCode != 200) {
      print('❌ Failed to connect to VM Service');
      print('   Status code: ${response.statusCode}');
      print('');
      print('   Make sure Flutter app is running with:');
      print(
        '   flutter run --dds-port=8181 --host-vmservice-port=8182 --disable-service-auth-codes',
      );
      exit(1);
    }

    final responseBody = await response.transform(utf8.decoder).join();
    final vmData = jsonDecode(responseBody);

    print('✅ Connected to VM Service');
    print('   VM version: ${vmData['version']}');
    print('');

    // Note: Full VM Service API integration requires the vm_service package
    // This is a basic example. For full functionality, use:
    // import 'package:vm_service/vm_service.dart';
    // import 'package:vm_service/vm_service_io.dart';
    //
    // final service = await vmServiceConnectUri(vmServiceUri);
    // final vm = await service.getVM();
    // final isolate = vm.isolates!.first;
    // final coverage = await service.getSourceReport(isolate.id!, ['Coverage']);

    print('💡 For full coverage collection, use the vm_service package:');
    print('   1. Add to pubspec.yaml dev_dependencies:');
    print('      vm_service: ^11.0.0');
    print('   2. Use VM Service API to collect coverage');
    print('   3. Save coverage data and process with format_coverage');
    print('');
    print('   See docs/CODE_COVERAGE.md for detailed instructions');
  } catch (e) {
    print('❌ Error connecting to VM Service: $e');
    print('');
    print('   Make sure:');
    print('   1. Flutter app is running with VM Service enabled');
    print('   2. VM Service URI is correct');
    print('   3. No firewall blocking the connection');
    exit(1);
  }
}
