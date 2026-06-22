// This file forces libray_api.so to be a NEEDED dependency of the cdylib.
// The actual linking is handled by --no-as-needed in build.rs.
// This file exists so that rayrust-sys's static lib has a reference to
// libray_api.so symbols, but the real forcing is done by the linker flag.
//
// We intentionally leave this minimal to avoid issues during dlopen.
