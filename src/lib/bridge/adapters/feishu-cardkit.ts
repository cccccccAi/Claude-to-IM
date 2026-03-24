/**
 * Feishu CardKit v1 API 封装模块
 *
 * 封装飞书 CardKit v1 流式卡片操作：创建卡片实体、发送卡片消息、
 * 流式更新文字内容（打字机效果）、关闭流式并全量替换最终内容。
 *
 * 设计约束：
 * - 所有函数 try/catch，失败返回 null/false，不抛异常
 * - 失败时 console.warn 记录，格式 [feishu-cardkit] xxx failed: message
 * - 无外部依赖，只用传入的 client 参数（类型 any，SDK 类型不完整）
 */

/** 流式内容区块的固定 element_id，与初始卡片模板对应。 */
export const STREAMING_ELEMENT_ID = "streaming_content";

/**
 * 初始流式卡片模板（schema 2.0）。
 * streaming_mode: true 开启打字机模式；
 * summary 用于通知折叠预览文案。
 */
const INITIAL_STREAMING_CARD = {
  schema: "2.0",
  config: {
    streaming_mode: true,
    wide_screen_mode: true,
    summary: { content: "思考中..." },
  },
  body: {
    elements: [
      {
        tag: "markdown",
        content: "",
        text_align: "left",
        text_size: "normal",
        element_id: STREAMING_ELEMENT_ID,
      },
    ],
  },
};

/**
 * 创建流式卡片实体（CardKit v1 card.create）。
 *
 * @param client - 飞书 SDK Client 对象
 * @returns cardId 字符串，失败返回 null
 */
export async function createStreamingCard(client: any): Promise<string | null> {
  try {
    const res = await client.cardkit.v1.card.create({
      data: {
        type: "card_json",
        data: JSON.stringify(INITIAL_STREAMING_CARD),
      },
    });

    const cardId: string | undefined = res?.data?.card_id;
    if (!cardId) {
      console.warn(
        "[feishu-cardkit] createStreamingCard failed: card_id missing in response",
      );
      return null;
    }
    return cardId;
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.warn(`[feishu-cardkit] createStreamingCard failed: ${message}`);
    return null;
  }
}

/**
 * 发送卡片消息到指定聊天（im.message.create）。
 * 使用 card_id 引用已创建的 CardKit 实体，而非内联卡片 JSON。
 *
 * @param client          - 飞书 SDK Client 对象
 * @param chatId          - 目标 chat_id
 * @param cardId          - CardKit 实体 card_id
 * @param replyToMessageId - 可选，回复的消息 ID（用于线程回复）
 * @returns messageId 字符串，失败返回 null
 */
export async function sendCardMessage(
  client: any,
  chatId: string,
  cardId: string,
  replyToMessageId?: string,
): Promise<string | null> {
  try {
    const params: Record<string, unknown> = {
      params: { receive_id_type: "chat_id" },
      data: {
        receive_id: chatId,
        msg_type: "interactive",
        content: JSON.stringify({ type: "card", data: { card_id: cardId } }),
      },
    };

    if (replyToMessageId) {
      // 飞书回复消息使用 reply API
      const replyRes = await client.im.message.reply({
        path: { message_id: replyToMessageId },
        data: {
          msg_type: "interactive",
          content: JSON.stringify({ type: "card", data: { card_id: cardId } }),
        },
      });
      const messageId: string | undefined = replyRes?.data?.message_id;
      if (!messageId) {
        console.warn(
          "[feishu-cardkit] sendCardMessage failed: message_id missing in reply response",
        );
        return null;
      }
      return messageId;
    }

    const res = await client.im.message.create(params);
    const messageId: string | undefined = res?.data?.message_id;
    if (!messageId) {
      console.warn(
        "[feishu-cardkit] sendCardMessage failed: message_id missing in response",
      );
      return null;
    }
    return messageId;
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.warn(`[feishu-cardkit] sendCardMessage failed: ${message}`);
    return null;
  }
}

/**
 * 流式更新卡片指定元素的文字内容（打字机效果）。
 * 对应 CardKit v1 cardElement.content API。
 *
 * @param client   - 飞书 SDK Client 对象
 * @param cardId   - CardKit 实体 card_id
 * @param content  - 当前累积的 Markdown 文本内容
 * @param sequence - 单调递增序列号，用于保序（从 1 开始）
 * @returns 成功返回 true，失败返回 false
 */
export async function streamContent(
  client: any,
  cardId: string,
  content: string,
  sequence: number,
): Promise<boolean> {
  try {
    await client.cardkit.v1.cardElement.content({
      path: {
        card_id: cardId,
        element_id: STREAMING_ELEMENT_ID,
      },
      data: { content, sequence },
    });
    return true;
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.warn(`[feishu-cardkit] streamContent failed: ${message}`);
    return false;
  }
}

/**
 * 关闭流式模式并全量替换最终卡片内容（两步操作，避免闪烁）。
 *
 * 步骤：
 * 1. card.settings 关闭 streaming_mode
 * 2. card.update  替换为最终卡片 JSON
 *
 * 两步共享同一 sequence 值（settings 用 sequence，update 用 sequence+1
 * 以确保 update 在 settings 之后被服务端接受）。
 *
 * @param client        - 飞书 SDK Client 对象
 * @param cardId        - CardKit 实体 card_id
 * @param finalCardJson - 最终卡片的完整 JSON 字符串（schema 2.0）
 * @param sequence      - 当前最大序列号，函数内部递增使用
 * @returns 两步均成功返回 true，任一失败返回 false
 */
export async function finalizeCard(
  client: any,
  cardId: string,
  finalCardJson: string,
  sequence: number,
): Promise<boolean> {
  // 步骤 1：关闭流式模式
  try {
    await client.cardkit.v1.card.settings({
      path: { card_id: cardId },
      data: {
        settings: JSON.stringify({ streaming_mode: false }),
        sequence,
      },
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.warn(`[feishu-cardkit] finalizeCard settings failed: ${message}`);
    return false;
  }

  // 步骤 2：全量替换卡片内容（sequence+1 确保服务端有序接受）
  try {
    await client.cardkit.v1.card.update({
      path: { card_id: cardId },
      data: {
        card: {
          type: "card_json",
          data: finalCardJson,
        },
        sequence: sequence + 1,
      },
    });
    return true;
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : String(err);
    console.warn(`[feishu-cardkit] finalizeCard update failed: ${message}`);
    return false;
  }
}
