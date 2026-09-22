FPV STEREO NEPAL UPLOADER
=========================

1. Extract the entire ZIP to a local folder.
2. Run PowerShell in that folder:
     powershell -ExecutionPolicy Bypass -File .\setup-nepal.ps1
3. Connect the data drive.
4. Upload its session folders:
     upload-nepal.cmd "E:\"

The uploader then asks for optional L1, L2, and L3 labels. Press Enter to leave
any value blank. These are free-text labels and are not checked against a taxonomy.

The uploader accepts one data folder or a parent containing multiple data
folders. It does not infer or require a device ID or naming convention. It
preserves each original session folder, every filename, and all nested folders.
It ignores ._* and operating-system metadata.

Duplicate checking uses the complete contents, relative filenames, and sizes of
the non-video files. Video files are uploaded and size-verified, but video bytes
are not read to calculate the content ID.

The included .env file is private. Do not publish or forward the ZIP beyond the
approved vendor operator.

Select the folder you want to preserve: its entire tree is uploaded together.
Video-only folders and zero-byte files are accepted. Video-only IDs use up to 384 KiB of samples per video and work across computers.

CHECK YOUR UPLOAD
Open https://fpvnepal.satpal-341.workers.dev and sign in with @fpvlabs.ai.
Other emails must be approved before they can sign in.
One selected SD card folder appears as one upload. You can browse its files,
see start/finish times, and check the Verified status. Retrying the same data
uses the same card receipt. Daily totals use Nepal time by default.
Video hours sum every camera file; simultaneous cameras count separately.
MP4/MOV durations are measured from headers. Unknown durations are marked
pending and never prevent upload. Use this updated package for live progress
and hours. Re-running an older completed upload adds its duration receipt.
