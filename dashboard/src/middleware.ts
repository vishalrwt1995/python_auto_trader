import { NextResponse } from "next/server";
import type { NextRequest } from "next/server";

const PUBLIC_PATHS = ["/login", "/_next", "/favicon.ico", "/manifest.json", "/icons", "/sw.js"];

export function middleware(request: NextRequest) {
  const { pathname } = request.nextUrl;

  // Allow public paths
  if (PUBLIC_PATHS.some((p) => pathname.startsWith(p))) {
    return NextResponse.next();
  }

  // Check for auth cookie (set by Firebase Auth persistence on client).
  // Since Firebase Auth is client-side, we use a lightweight cookie check.
  // The actual token validation happens on API calls.
  const session = request.cookies.get("__session");
  if (!session?.value && pathname !== "/login") {
    // For the initial load without cookie, allow client-side auth to handle it.
    // The AuthProvider will redirect if not authenticated.
    return NextResponse.next();
  }

  return NextResponse.next();
}

export const config = {
  matcher: ["/((?!api|_next/static|_next/image|favicon.ico).*)"],
};
