import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { API_BASE, api, authFetch } from "@/lib/api";

interface Channel {
  channel_id: string;
  name: string;
  platform: string;
  is_member: boolean;
}

interface Source {
  number: number;
  fact_id: string;
  channel_id: string;
  channel_name: string;
  platform: string;
  author: string;
  timestamp: string;
  url: string;
  text: string;
}

interface WorkspaceAnswer {
  answer: string;
  sources: Source[];
  cited_source_numbers: number[];
  searched_channels: string[];
  extraction_incomplete: boolean;
}

export function WorkspaceAskPage() {
  const [params] = useSearchParams();
  const [question, setQuestion] = useState(params.get("q") ?? "");
  const [channels, setChannels] = useState<Channel[]>([]);
  const [selected, setSelected] = useState<string[]>([]);
  const [answer, setAnswer] = useState<WorkspaceAnswer | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  useEffect(() => {
    Promise.all([
      api.get<Channel[]>("/api/channels"),
      api.get<{ channel_ids: string[] }>("/api/ask/workspace/channels"),
    ])
      .then(([items, scope]) => {
        const authorized = new Set(scope.channel_ids);
        setChannels(items.filter((channel) => channel.is_member && authorized.has(channel.channel_id)));
      })
      .catch(() => setChannels([]));
  }, []);

  async function ask(event: FormEvent) {
    event.preventDefault();
    if (!question.trim() || loading) return;
    setLoading(true);
    setError("");
    setAnswer(null);
    try {
      const response = await authFetch(`${API_BASE}/api/ask/workspace`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          question: question.trim(),
          ...(selected.length ? { channel_ids: selected } : {}),
        }),
      });
      if (!response.ok) {
        setError(response.status === 429
          ? "The answer provider is rate limited. Try again shortly."
          : "The workspace answer is unavailable right now.");
        return;
      }
      setAnswer(await response.json() as WorkspaceAnswer);
    } catch {
      setError("The workspace answer is unavailable right now.");
    } finally {
      setLoading(false);
    }
  }

  function toggle(channelId: string) {
    setSelected((current) => current.includes(channelId)
      ? current.filter((id) => id !== channelId)
      : [...current, channelId]);
  }

  return (
    <div className="h-full overflow-auto">
      <div className="max-w-3xl mx-auto p-6 sm:p-10 space-y-6">
        <div>
          <h1 className="font-heading text-3xl text-foreground">Ask across your workspace</h1>
          <p className="text-muted-foreground mt-2">
            Search all indexed channels you can access. Choose channels only when you want to narrow the answer.
          </p>
        </div>

        <form onSubmit={ask} className="space-y-3">
          <label htmlFor="workspace-question" className="sr-only">Question</label>
          <textarea
            id="workspace-question"
            value={question}
            onChange={(event) => setQuestion(event.target.value)}
            placeholder="What would you like to know?"
            rows={3}
            className="w-full rounded-xl border border-border bg-card p-4 text-foreground"
          />
          <div className="flex items-center justify-between gap-3">
            <details>
              <summary className="cursor-pointer text-sm text-muted-foreground">
                {selected.length ? `${selected.length} channel filters` : "All authorized indexed channels"}
              </summary>
              <div className="mt-3 max-h-48 overflow-auto rounded-lg border border-border bg-card p-3 space-y-2">
                {channels.map((channel) => (
                  <label key={channel.channel_id} className="flex gap-2 text-sm text-foreground">
                    <input type="checkbox" checked={selected.includes(channel.channel_id)}
                      onChange={() => toggle(channel.channel_id)} />
                    {channel.name}
                  </label>
                ))}
              </div>
            </details>
            <button type="submit" disabled={loading || !question.trim()}
              className="rounded-lg bg-primary px-4 py-2 text-primary-foreground disabled:opacity-50">
              {loading ? "Finding evidence..." : "Ask"}
            </button>
          </div>
        </form>

        {error && <p role="alert" className="text-destructive">{error}</p>}
        {answer && (
          <div className="space-y-5">
            {answer.extraction_incomplete && (
              <p className="rounded-lg border border-amber-500/30 bg-amber-500/10 p-3 text-sm text-foreground">
                Some selected channels are still being indexed. This answer may miss recent or older messages.
              </p>
            )}
            <div className="whitespace-pre-wrap text-foreground leading-relaxed">{answer.answer}</div>
            {!!answer.sources.length && (
              <div>
                <h2 className="font-medium text-foreground">Sources</h2>
                <ol className="mt-2 space-y-3">
                  {answer.sources.filter((source) => answer.cited_source_numbers.includes(source.number))
                    .map((source) => (
                      <li key={source.fact_id} className="rounded-lg border border-border bg-card p-3 text-sm">
                        <div className="font-medium text-foreground">
                          [{source.number}] {source.channel_name} · {source.timestamp}
                        </div>
                        <p className="mt-1 text-muted-foreground">{source.text}</p>
                        {source.url && <a href={source.url} target="_blank" rel="noreferrer"
                          className="mt-1 inline-block text-primary underline">Open source message</a>}
                      </li>
                    ))}
                </ol>
              </div>
            )}
          </div>
        )}
        <Link to="/ask?new=1" className="text-sm text-primary underline">
          Open channel conversation
        </Link>
      </div>
    </div>
  );
}
