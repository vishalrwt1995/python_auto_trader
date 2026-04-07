import type { Metadata, Viewport } from "next";
import { Inter, JetBrains_Mono } from "next/font/google";
import "@/styles/globals.css";
import { AuthProvider } from "@/components/providers/AuthProvider";
import { FirestoreProvider } from "@/components/providers/FirestoreProvider";
import { Sidebar } from "@/components/layout/Sidebar";
import { Topbar } from "@/components/layout/Topbar";
import { MobileNav } from "@/components/layout/MobileNav";
import { ErrorBoundary } from "@/components/shared/ErrorBoundary";

const inter = Inter({ subsets: ["latin"], variable: "--font-sans" });
const jetbrains = JetBrains_Mono({
  subsets: ["latin"],
  variable: "--font-mono",
});

export const metadata: Metadata = {
  title: "SmartTrader Dashboard",
  description: "Real-time trading dashboard — SmartTrader GCP",
  manifest: "/manifest.json",
};

export const viewport: Viewport = {
  themeColor: "#0a0e17",
  width: "device-width",
  initialScale: 1,
  maximumScale: 1,
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <body className={`${inter.variable} ${jetbrains.variable} font-sans`}>
        <AuthProvider>
          <FirestoreProvider>
            <Sidebar />
            <Topbar />
            <main className="md:ml-[220px] pt-[52px] pb-16 md:pb-0 min-h-screen">
              <ErrorBoundary>
                <div style={{ padding: 20 }}>{children}</div>
              </ErrorBoundary>
            </main>
            <MobileNav />
          </FirestoreProvider>
        </AuthProvider>
      </body>
    </html>
  );
}
