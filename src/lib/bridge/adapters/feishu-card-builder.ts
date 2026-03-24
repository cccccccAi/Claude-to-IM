/**
 * 飞书卡片内容构建模块
 * 用于 streaming 结束后生成全量替换的最终卡片 JSON
 */

/**
 * 格式化耗时显示
 * - < 1000ms → "0.Xs"
 * - < 60000ms → "X.Xs"
 * - >= 60000ms → "Xm Xs"
 */
export function formatElapsed(ms: number): string {
  if (ms < 1000) {
    return `0.${Math.floor(ms / 100)}s`;
  }
  if (ms < 60000) {
    const seconds = ms / 1000;
    return `${seconds.toFixed(1)}s`;
  }
  const minutes = Math.floor(ms / 60000);
  const seconds = Math.floor((ms % 60000) / 1000);
  return `${minutes}m ${seconds}s`;
}

/**
 * 飞书 markdown 预处理：
 * - strip HTML 标签（<br> → \n，其余 tag 去掉保留内容）
 * - 代码块（```...```）内部内容保持原样不处理
 */
export function preprocessFeishuMarkdown(text: string): string {
  // 拆分成代码块和非代码块两类片段，分开处理
  const segments: Array<{ isCode: boolean; content: string }> = [];
  const codeBlockPattern = /```[\s\S]*?```/g;

  let lastIndex = 0;
  let match: RegExpExecArray | null;

  while ((match = codeBlockPattern.exec(text)) !== null) {
    // 代码块之前的普通文本
    if (match.index > lastIndex) {
      segments.push({
        isCode: false,
        content: text.slice(lastIndex, match.index),
      });
    }
    // 代码块本身，保持原样
    segments.push({ isCode: true, content: match[0] });
    lastIndex = match.index + match[0].length;
  }

  // 剩余普通文本
  if (lastIndex < text.length) {
    segments.push({ isCode: false, content: text.slice(lastIndex) });
  }

  return segments
    .map(({ isCode, content }) => {
      if (isCode) return content;
      // strip HTML：先把 <br> / <br/> 换成换行，再去掉其余标签
      return content.replace(/<br\s*\/?>/gi, "\n").replace(/<[^>]+>/g, "");
    })
    .join("");
}

/**
 * 构建最终卡片 JSON（streaming 结束后全量替换用）。
 * 只包含干净的 markdown 内容 + 可选 footer，不含工具进度。
 */
export function buildFinalCardJson(
  text: string,
  options?: {
    elapsed?: number; // 耗时毫秒
    isError?: boolean; // 是否是错误状态
  },
): string {
  const { elapsed, isError = false } = options ?? {};

  const processedText = preprocessFeishuMarkdown(text.trim() || "No response");

  const elements: object[] = [
    {
      tag: "markdown",
      content: processedText,
      text_align: "left",
      text_size: "normal",
    },
  ];

  // 有 elapsed 才追加 footer
  if (elapsed !== undefined) {
    const timeStr = formatElapsed(elapsed);
    const footerContent = isError
      ? `❌ Error · ${timeStr}`
      : `✅ Completed · ${timeStr}`;

    elements.push({ tag: "hr" });
    elements.push({
      tag: "markdown",
      content: footerContent,
      text_size: "notation",
    });
  }

  const card = {
    schema: "2.0",
    config: { wide_screen_mode: true },
    body: { elements },
  };

  return JSON.stringify(card);
}
