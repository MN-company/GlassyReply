
import { version } from '../package.json';

export async function sendTelegramNotification(
	botToken: string,
	chatId: string,
	text: string,
	replyToMessageId?: number
): Promise<void> {
	const url = `https://api.telegram.org/bot${botToken}/sendMessage`;
	const requestBodyParams: { chat_id: string; text: string; disable_notification: string; reply_to_message_id?: number } = {
		chat_id: chatId,
		text,
		disable_notification: 'false',
	};
	if (replyToMessageId) {
		requestBodyParams.reply_to_message_id = replyToMessageId;
	}

	const requestBody = new URLSearchParams(requestBodyParams as Record<string, string>);

	console.log(`DEBUG: Telegram API URL: ${url}`);
	console.log(`DEBUG: Telegram Request Body: ${requestBody.toString()}`);

	const response = await fetch(url, {
		method: 'POST',
		headers: {
			'Content-Type': 'application/x-www-form-urlencoded',
		},
		body: requestBody,
	});

	if (!response.ok) {
		const errorBody = await response.text();
		console.error(`Telegram API Error: Status ${response.status}, ${response.statusText}, Body: ${errorBody}`);
		throw new Error(`Telegram notification failed: ${response.statusText} - ${errorBody}`);
	}
}

export async function logPixelOpen(
	kv: KVNamespace,
	id: string,
	ip: string | null,
	userAgent: string | null,
	gmailFetchInfo: string | null
): Promise<void> {
	await kv.put(`${id}-${Date.now()}`, JSON.stringify({
		ip,
		userAgent,
		gmailFetchInfo
	}));
}
