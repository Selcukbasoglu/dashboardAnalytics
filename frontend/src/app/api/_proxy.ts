export async function readJSON(res: Response): Promise<{ data: any; error?: string }> {
  const text = await res.text();
  if (!text) {
    return { data: { error: "upstream_empty_response" }, error: "empty" };
  }
  try {
    return { data: JSON.parse(text) };
  } catch (err) {
    return {
      data: { error: "upstream_invalid_json", detail: String(err), raw: text.slice(0, 200) },
      error: "invalid_json",
    };
  }
}

export async function proxyJSON(url: string, init?: RequestInit): Promise<{ status: number; data: any }> {
  try {
    const res = await fetch(url, init);
    const { data } = await readJSON(res);
    return { status: res.status, data };
  } catch (err) {
    return { status: 502, data: { error: "upstream_unreachable", detail: String(err) } };
  }
}
