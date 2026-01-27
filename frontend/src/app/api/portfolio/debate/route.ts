import { NextResponse } from "next/server";
import { proxyJSON } from "@/app/api/_proxy";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";

async function proxy(req: Request) {
  const { search } = new URL(req.url);
  const { status, data } = await proxyJSON(`${API_BASE}/api/v1/portfolio/debate${search}`, {
    method: req.method,
    cache: "no-store",
    body: req.method === "POST" ? await req.text() : undefined,
    headers: req.method === "POST" ? { "Content-Type": "application/json" } : undefined,
  });
  return NextResponse.json(data, { status });
}

export async function GET(req: Request) {
  return proxy(req);
}

export async function POST(req: Request) {
  return proxy(req);
}
