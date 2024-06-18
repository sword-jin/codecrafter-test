use bytes::BufMut;
use clap::Parser;
use std::net::SocketAddrV4;
use std::net::UdpSocket;
use std::vec;

#[derive(Debug, Parser)]
pub struct Args {
    #[arg(short = 'r', long)]
    pub resolver: Option<SocketAddrV4>,
}

fn main() {
    let udp_socket = UdpSocket::bind("127.0.0.1:2053").expect("Failed to bind to address");

    let args = Args::parse();
    let is_forwarded_server = args.resolver.is_some();
    let mut buf = [0; 512];

    loop {
        match udp_socket.recv_from(&mut buf) {
            Ok((_, source)) => {
                let mut message = Message::from_bytes(&buf);

                let response = if is_forwarded_server {
                    let resolver_addr = args.resolver.unwrap();
                    let messages = message.questions.into_iter().map(|q| {
                        let mut new_message = Message::default();
                        new_message.header = message.header.clone();
                        new_message.questions.push(q);
                        new_message.set_qrcount(1);
                        new_message
                    });

                    let response_messages: Vec<_> = messages
                        .into_iter()
                        .map(|m| {
                            let s = m.to_bytes();
                            println!("{:>02x?}", s);
                            udp_socket
                                .send_to(&s, resolver_addr)
                                .expect("Failed to send data");
                            println!("Sent query to resolver: {:?}", resolver_addr);
                            let mut buf = [0; 512];
                            let (n, _) = udp_socket
                                .recv_from(&mut buf)
                                .expect("Failed to receive data");
                            println!("Received response from resolver: {:?}", resolver_addr);
                            Message::from_bytes(&buf[..n])
                        })
                        .collect();

                    let mut new_message = Message::default();
                    new_message.header = response_messages[0].header.clone();
                    new_message.add_answers(
                        response_messages
                            .iter()
                            .flat_map(|m| m.answers.clone())
                            .collect(),
                    );
                    new_message.add_questions(
                        response_messages
                            .iter()
                            .flat_map(|m| m.questions.clone())
                            .collect(),
                    );

                    new_message.to_bytes()
                } else {
                    let mut answers = Vec::with_capacity(message.questions.len() as usize);
                    for q in message.questions.iter() {
                        answers.push(Answer {
                            name: q.qname.clone(),
                            atype: q.qtype,
                            aclass: q.qclass,
                            ttl: 60,
                            rdlength: 4,
                            rdata: vec![127, 0, 0, 1],
                        });
                    }

                    message.set_qrcount(message.questions.len() as u16);
                    message.add_answers(answers);
                    message.set_id(message.header.id);
                    message.set_opcode(message.header.opcode);
                    message.set_rd(message.header.rd);
                    message.set_qr(1);
                    message.set_rcode(if 0 == message.header.opcode {
                        0
                    } else {
                        4 /* not implement */
                    });

                    message.to_bytes()
                };

                udp_socket
                    .send_to(response.as_slice(), source)
                    .expect("Failed to send data");
            }
            Err(e) => {
                eprintln!("Error receiving data: {}", e);
                break;
            }
        }
    }
}

#[derive(Debug, Clone)]
struct Header {
    // A random ID assigned to query packets. Response packets must reply with the same ID.
    id: u16,
    // 1 for a reply packet, 0 for a question packet.
    qr: u8,
    // Specifies the kind of query in a message.
    opcode: u8,
    // 1 if the responding server "owns" the domain queried, i.e., it's authoritative.
    aa: u8,
    // 1 if the message is larger than 512 bytes. Always 0 in UDP responses.
    tc: u8,
    // Sender sets this to 1 if the server should recursively resolve this query, 0 otherwise.
    rd: u8,
    // Server sets this to 1 to indicate that recursion is available.
    ra: u8,
    // Reserved for future use.  Must be zero in all queries and responses.
    z: u8,
    // Response code indicating the status of the response.
    rcode: u8,
    // Number of questions in the Question section.
    qrcount: u16,
    // Number of records in the Answer section.
    ancount: u16,
    // Number of records in the Authority section.
    nscount: u16,
    // Number of records in the Additional section.
    arcount: u16,
}

impl Default for Header {
    fn default() -> Self {
        Header {
            id: 1234,
            qr: 1,
            opcode: 0,
            aa: 0,
            tc: 0,
            rd: 0,
            ra: 0,
            z: 0,
            rcode: 0,
            qrcount: 0,
            ancount: 0,
            nscount: 0,
            arcount: 0,
        }
    }
}

impl Header {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(12);
        bytes.extend_from_slice(&self.id.to_be_bytes()); // 2 bytes
        bytes.push(
            (self.qr << 7)
                | ((self.opcode & 0b1111) << 3)
                | (self.aa << 2)
                | (self.tc << 1)
                | self.rd,
        ); // 1 byte
        bytes.push((self.ra << 7) | (self.z << 4) | self.rcode);
        bytes.put_u16(self.qrcount);
        bytes.put_u16(self.ancount);
        bytes.put_u16(self.nscount);
        bytes.put_u16(self.arcount);
        bytes
    }

    fn from_bytes(buf: &[u8]) -> Self {
        assert_eq!(buf.len(), 12);

        println!("{:?}", buf);

        Header {
            id: u16::from_be_bytes([buf[0], buf[1]]),
            qr: buf[2] >> 7 & 0b00000001,
            opcode: buf[2] >> 3 & 0b00000111,
            aa: buf[2] >> 2 & 0b00000001,
            tc: buf[2] >> 1 & 0b00000001,
            rd: buf[2] >> 1 & 0b00000001,
            ra: buf[3] >> 7 & 0b00000001,
            z: buf[3] >> 4 & 0b00000111,
            rcode: buf[3] & 0b00001111,
            qrcount: u16::from_be_bytes([buf[4], buf[5]]),
            ancount: u16::from_be_bytes([buf[6], buf[7]]),
            nscount: u16::from_be_bytes([buf[8], buf[9]]),
            arcount: u16::from_be_bytes([buf[10], buf[11]]),
        }
    }
}

#[derive(Debug, Clone)]
struct Labels(Vec<String>);

impl Labels {
    fn from_bytes(buf: &[u8], mut start: usize) -> (Self, usize) {
        let mut labels = Vec::new();
        loop {
            match buf[start] {
                zero if zero == b'\0' => {
                    start += 1;
                    break;
                }
                p if p & 0b1100_0000 == 0b1100_0000 => {
                    let offset = u16::from_be_bytes([p, buf[start + 1]]) ^ 0b1100_0000_0000_0000;
                    start += 2;
                    let (label, _) = Self::from_bytes(buf, offset as usize);
                    labels.extend(label.0);
                    break;
                }
                len => {
                    let len = len as usize;
                    start += 1;
                    labels.push(String::from_utf8_lossy(&buf[start..start + len]).to_string());
                    start += len;
                }
            }
        }
        (Self(labels), start)
    }
}

#[derive(Debug, Clone)]
struct Question {
    qname: Labels,
    qtype: u16,
    qclass: u16,
}

impl Question {
    fn parse_bytes(buf: &[u8], start: usize) -> (Self, usize) {
        // The domain name terminates with the zero length octet for the null label of the root.
        let (qname, start) = Labels::from_bytes(buf, start);

        let qtype = u16::from_be_bytes([buf[start], buf[start + 1]]);
        let qclass = u16::from_be_bytes([buf[start + 2], buf[start + 3]]);
        (
            Self {
                qname,
                qtype,
                qclass,
            },
            start + 4,
        )
    }

    fn as_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        for label in self.qname.0.iter() {
            buf.push(label.len() as u8);
            buf.extend_from_slice(label.as_bytes());
        }

        buf.put_u8(0); // terminating null label
        buf.extend_from_slice(&self.qtype.to_be_bytes());
        buf.extend_from_slice(&self.qclass.to_be_bytes());
        buf
    }
}

#[derive(Debug, Clone)]
struct Message {
    header: Header,
    questions: Vec<Question>,
    answers: Vec<Answer>,
}

impl Default for Message {
    fn default() -> Self {
        Message {
            header: Header::default(),
            questions: Vec::new(),
            answers: Vec::new(),
        }
    }
}

impl Message {
    fn from_bytes(buf: &[u8]) -> Self {
        let header = Header::from_bytes(&buf[..12]);
        let mut questions = Vec::with_capacity(header.qrcount as usize);

        let mut start = 12;
        for _i in 0..header.qrcount {
            let (question, n) = Question::parse_bytes(&buf, start);
            start = n;
            questions.push(question);
        }

        Self {
            header,
            questions,
            ..Default::default()
        }
    }

    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.header.to_bytes();
        for question in &self.questions {
            let s = question.as_bytes();
            bytes.extend_from_slice(&s);
        }

        for answer in &self.answers {
            bytes.extend_from_slice(answer.to_bytes().as_slice());
        }

        bytes
    }

    fn add_answers(&mut self, answers: Vec<Answer>) {
        self.header.ancount = answers.len() as u16;
        self.answers.extend(answers);
    }

    fn add_questions(&mut self, questions: Vec<Question>) {
        self.header.ancount = questions.len() as u16;
        self.questions.extend(questions);
    }

    fn set_qrcount(&mut self, qrcount: u16) {
        self.header.qrcount = qrcount;
    }

    fn set_id(&mut self, id: u16) {
        self.header.id = id;
    }

    fn set_rd(&mut self, rd: u8) {
        self.header.rd = rd;
    }

    fn set_opcode(&mut self, code: u8) {
        self.header.opcode = code;
    }

    fn set_qr(&mut self, qr: u8) {
        self.header.qr = qr;
    }

    fn set_rcode(&mut self, rcode: u8) {
        self.header.rcode = rcode;
    }
}

#[derive(Debug, Clone)]
struct Answer {
    name: Labels,
    atype: u16,
    aclass: u16,
    ttl: u32,
    rdlength: u16,
    rdata: Vec<u8>,
}

impl Answer {
    fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        // Convert the fields of the Answer struct to bytes and append them to the bytes vector
        for label in self.name.0.iter() {
            buf.push(label.len() as u8);
            buf.extend_from_slice(label.as_bytes());
        }
        buf.put_u8(0); // terminating null label
        buf.put_u16(self.atype);
        buf.put_u16(self.aclass);
        buf.put_u32(self.ttl);
        buf.put_u16(self.rdlength);
        buf.extend_from_slice(&self.rdata);
        buf
    }
}
