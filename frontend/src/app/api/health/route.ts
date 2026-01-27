import { NextResponse } from "next/server";
import { proxyJSON } from "@/app/api/_proxy";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";

export async function GET() {
  const { status, data } = await proxyJSON(`${API_BASE}/api/v1/health`, { cache: "no-store" });
  return NextResponse.json(data, { status });
}
