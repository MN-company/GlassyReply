import { Env } from './types';

const GIF = Uint8Array.from(
  atob('R0lGODlhAQABAAAAACw='),
  (c) => c.charCodeAt(0)
);

export default {
  async fetch(request: Request, env: Env, ctx: ExecutionContext): Promise<Response> {
    const url = new URL(request.url);

    if (url.pathname === '/pixel') {
      const id = url.searchParams.get('id') || crypto.randomUUID();
      const tgMsgId = url.searchParams.get('tg_msg_id');
      const emailSubject = url.searchParams.get('subj');
      const emailBodyExcerpt = url.searchParams.get('body_ex');

      const ip = request.headers.get('cf-connecting-ip');
      const userAgent = request.headers.get('User-Agent');
      const gmailFetchInfo = request.headers.get('X-Gmail-Fetch-Info');

      let isBotOrProxy = false;
      if (userAgent) {
        const lowerCaseUserAgent = userAgent.toLowerCase();
        if (lowerCaseUserAgent.includes('googleimageproxy') || lowerCaseUserAgent.includes('bot')) {
          isBotOrProxy = true;
        }
      }

      // Prepara i dati da inviare al webhook
      const webhookData = {
        tg_msg_id: tgMsgId,
        is_user_open: !isBotOrProxy, // True se non è un bot/proxy
        email_subject: emailSubject ? decodeURIComponent(emailSubject) : 'N/A',
        email_body_excerpt: emailBodyExcerpt ? decodeURIComponent(emailBodyExcerpt) : 'N/A',
        pixel_id: id,
        ip: ip,
        user_agent: userAgent,
        gmail_fetch_info: gmailFetchInfo,
      };

      try {
        // Invia la richiesta al webhook del bot Python
        const webhookResponse = await fetch(env.PIXEL_WEBHOOK_URL, {
          method: 'POST',
          headers: {
            'Content-Type': 'application/json',
            'X-Pixel-Secret': env.PIXEL_WEBHOOK_SECRET, // Chiave segreta per autenticazione
          },
          body: JSON.stringify(webhookData),
        });

        if (!webhookResponse.ok) {
          const errorBody = await webhookResponse.text();
          console.error(`Webhook call failed: Status ${webhookResponse.status}, ${webhookResponse.statusText}, Body: ${errorBody}`);
        }
      } catch (error) {
        console.error('Failed to call webhook:', error);
      }

      return new Response(GIF, {
        headers: {
          'content-type': 'image/gif',
          'cache-control': 'no-store',
        },
      });
    }

    return new Response('Not Found', { status: 404 });
  },
};