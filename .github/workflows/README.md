# GitHub Actions Workflows

## ci.yml

Consolidated CI/CD workflow for the Flutter + flutter_rust_bridge project.

### Workflow Behavior

The workflow adapts based on the trigger:

#### On Pull Requests
**Fast validation** - runs in ~5-10 minutes:
- ✅ Rust formatting check (`cargo fmt`)
- ✅ Rust linting (`cargo clippy`)
- ✅ Rust unit tests (`cargo test`)
- ✅ FRB bindings generation verification
- ✅ Flutter code analysis (`flutter analyze`)
- ✅ Flutter tests (`flutter test`)
- ✅ Quick builds: Linux (release) + Android (debug)

**Purpose**: Fast feedback for developers - catches most issues quickly.

#### On Main/Master Branch
**Full release builds** - runs in ~30-60 minutes:
- All PR checks PLUS:
- 📦 Android APK (release, unsigned)
- 📦 Android AAB (release, unsigned) - for Google Play Store
- 📦 iOS (release, unsigned)
- 📦 macOS (release)
- 📦 Linux (release)
- 📦 Windows (release)
- 📤 Uploads all artifacts (30-day retention)

**Purpose**: Production-ready builds for releases.

### Jobs

1. **rust-checks**
   - Runs formatting, linting, and tests on Rust code
   - Excludes `rust_lib_holon` (FRB wrapper) from tests

2. **flutter-integration**
   - Verifies FRB integration works correctly
   - Generates bindings and runs build_runner
   - Runs Flutter analysis and tests

3. **quick-build** (PR only)
   - Builds Linux and Android to catch platform-specific issues
   - Uses debug build for Android (faster)
   - Skips artifact upload

4. **full-build** (main/master only)
   - Matrix builds all 6 platforms
   - Release builds with artifacts
   - Creates iOS IPA from app bundle

5. **ci-complete**
   - Summary job for basic checks
   - Always runs to report status

6. **build-complete** (main/master only)
   - Summary job for full builds
   - Only runs after platform builds

### Platform Requirements

| Platform       | Runner           | Notes                         |
|----------------|------------------|-------------------------------|
| Android        | ubuntu-latest    | Requires Java 17              |
| iOS            | macos-latest     | Unsigned (--no-codesign)      |
| macOS          | macos-latest     |                               |
| Linux          | ubuntu-latest    | Requires GTK3 and build deps  |
| Windows        | windows-latest   |                               |

### Artifacts

Build artifacts (main/master builds only):

| Artifact Name          | Contents                              | Use Case               |
|------------------------|---------------------------------------|------------------------|
| `android-apk`          | `app-release.apk`                     | Direct installation    |
| `android-aab`          | `app-release.aab`                     | Google Play upload     |
| `ios-app`              | `Runner.app` + `holon.ipa`  | Testing/TestFlight     |
| `macos-app`            | `.app` bundle                         | macOS distribution     |
| `linux-bundle`         | Complete Linux bundle                 | Linux distribution     |
| `windows-executable`   | `.exe` and DLLs                       | Windows distribution   |

Retention: 30 days

### Triggers

```yaml
# Automatic triggers
- Push to main/master (only if changes in relevant paths)
- Pull requests to main/master (only if changes in relevant paths)

# Paths that trigger the workflow:
- frontends/flutter/**
- crates/holon/**
- .github/workflows/ci.yml

# Manual trigger
- workflow_dispatch (via GitHub UI)
```

### Caching Strategy

The workflow uses aggressive caching to speed up builds:

1. **Rust caching** (per platform):
   - `~/.cargo/bin/`
   - `~/.cargo/registry/`
   - `~/.cargo/git/`
   - `target/` directory

2. **Flutter caching** (per platform):
   - `~/.pub-cache/`
   - Built-in Flutter SDK cache

3. **Cache keys**:
   - Based on `Cargo.lock` and `pubspec.lock`
   - Platform-specific to avoid conflicts

### Environment Variables

- `FLUTTER_VERSION`: `3.27.2` - Update to match your Flutter version
- `RUST_VERSION`: `stable` - Can pin to specific version if needed
- `FRB_VERSION`: `2.11.1` - Must match version in `pubspec.yaml`

### Adding Android Signing

To sign Android builds for release distribution:

1. **Generate a keystore**:
   ```bash
   keytool -genkey -v -keystore key.jks -keyalg RSA -keysize 2048 -validity 10000 -alias release
   ```

2. **Create GitHub secrets**:
   - `ANDROID_KEYSTORE_BASE64`: `base64 -i key.jks | pbcopy`
   - `KEYSTORE_PASSWORD`: Your keystore password
   - `KEY_PASSWORD`: Your key password
   - `KEY_ALIAS`: `release` (or your alias)

3. **Add signing step** to workflow before Android build:
   ```yaml
   - name: Setup Android signing
     if: contains(matrix.platform, 'android')
     working-directory: frontends/flutter
     run: |
       echo "${{ secrets.ANDROID_KEYSTORE_BASE64 }}" | base64 --decode > android/app/key.jks
       cat > android/key.properties << EOF
       storePassword=${{ secrets.KEYSTORE_PASSWORD }}
       keyPassword=${{ secrets.KEY_PASSWORD }}
       keyAlias=${{ secrets.KEY_ALIAS }}
       storeFile=key.jks
       EOF
   ```

4. **Update `android/app/build.gradle`** to use `key.properties` for signing.

### Troubleshooting

**Issue**: FRB generation fails
- Check `flutter_rust_bridge.yaml` configuration
- Ensure FRB version matches in workflow and `pubspec.yaml`
- Verify Rust API uses FRB-compatible types

**Issue**: Platform build fails
- Check the specific platform's job logs
- Verify platform-specific dependencies are installed
- For Linux: Ensure all GTK3 dependencies are listed
- For Android: Check Java version (must be 17)

**Issue**: Dependency resolution fails
- Ensure `pubspec_overrides.yaml` is gitignored (it is)
- Check that `outliner_view` GitHub URL is accessible
- Verify all dependencies in `pubspec.yaml` are available

**Issue**: Caching not working
- Check cache keys match your lock files
- Verify paths are correct for your project structure
- Consider clearing cache via GitHub UI if corrupted

**Issue**: Workflow doesn't trigger
- Check if changes are in trigger paths
- Verify branch name matches (main/master)
- Check workflow file syntax with `yamllint`

### Performance Tips

1. **For faster PRs**: The quick builds only test Linux + Android debug
2. **For faster main builds**: Consider removing platforms you don't actively use
3. **For debugging**: Use `workflow_dispatch` to trigger manually without pushing
4. **For testing changes**: Fork first, test there, then open PR

### Monitoring

View workflow runs:
- Repository → Actions tab
- Filter by workflow name "CI/CD"
- Click on specific run to see detailed logs
- Download artifacts from successful builds

### Future Improvements

Potential enhancements:
- Add iOS code signing for TestFlight uploads
- Implement automatic version bumping
- Add release creation on tags
- Include code coverage reports
- Add performance benchmarking
- Implement automatic deployment to stores
