use reqwest::Response;
use serde_json::Value;
use tokio::time::timeout;

use crate::PATIENCE;

pub struct Stream {
    response: Response,
    buffer: String,
}

#[derive(Debug)]
pub struct Event {
    pub name: String,
    pub data: Value,
}

impl Stream {
    pub(crate) fn new(response: Response) -> Self {
        Self {
            response,
            buffer: String::new(),
        }
    }

    pub async fn next(&mut self) -> Event {
        self.find(|_| true).await
    }

    pub async fn find(&mut self, wanted: impl Fn(&Event) -> bool) -> Event {
        let path = self.response.url().path().to_owned();
        let found = async {
            loop {
                let event = self.read().await;
                if wanted(&event) {
                    return event;
                }
            }
        };
        timeout(PATIENCE, found)
            .await
            .unwrap_or_else(|_| panic!("{path} never sent the event"))
    }

    async fn read(&mut self) -> Event {
        loop {
            if let Some(event) = self.parse() {
                return event;
            }
            let chunk = self
                .response
                .chunk()
                .await
                .expect("an event stream chunk")
                .unwrap_or_else(|| panic!("the stream ended, holding {:?}", self.buffer));
            self.buffer.push_str(&String::from_utf8_lossy(&chunk));
        }
    }

    fn parse(&mut self) -> Option<Event> {
        while let Some(end) = self.buffer.find("\n\n") {
            let frame: String = self.buffer.drain(..end + 2).collect();
            let Some(data) = frame.lines().find_map(|line| line.strip_prefix("data:")) else {
                continue;
            };
            let name = frame
                .lines()
                .find_map(|line| line.strip_prefix("event:"))
                .unwrap_or("message")
                .trim()
                .to_owned();
            let data = serde_json::from_str(data.trim())
                .unwrap_or_else(|error| panic!("sse data is not json ({error}): {data}"));
            return Some(Event { name, data });
        }
        None
    }
}
