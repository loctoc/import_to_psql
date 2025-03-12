import axios from 'axios';
export async function sendSlackMessage(blocks) {
    if (options.slackNotifyUrl) {
      const body = {
        blocks,
      };
      try {
        await axios.post(options.slackNotifyUrl, body, {
          headers: {
            "Content-Type": "application/json",
          },
        });
      } catch (error) {
        console.error(
          "An error occurred while sending Slack notification:",
          error,
          error.stack
        );
      }
    }
  }