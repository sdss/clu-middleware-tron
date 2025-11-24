/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-11-22
 *  @Filename: parser.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

use regex::bytes::Regex;
use serde_json::Value;
use std::{collections::BTreeMap, str::FromStr, string::FromUtf8Error};

/// Represents a parsed reply from a Tron-style bytes string.
pub(crate) struct Reply {
    pub commander: u16,
    pub command_id: u32,
    pub code: char,
    pub keywords: BTreeMap<String, serde_json::Value>,
}

/// Adds a key and its associated values to the `keywords` field of a [Reply] struct.
fn add_to_reply_keywords(
    reply: &mut Reply,
    key: &mut Vec<u8>,
    raw_values: &mut Vec<Vec<u8>>,
    has_equal: bool,
) -> Result<(), FromUtf8Error> {
    // If the key is empty, do nothing.
    if key.is_empty() {
        return Ok(());
    }

    // Convert key to string.
    let key_string = String::from_utf8(key.clone())?;

    // If there are no value or if the keyword was of the form "key;" (no equal sign), set to Null.
    // Otherwise, parse each raw value into a serde_json::Value.
    if raw_values.is_empty() || !has_equal {
        reply.keywords.insert(key_string, Value::Null);
    } else {
        let mut values: Vec<Value> = Vec::new();
        for raw_value in raw_values.iter() {
            let raw_str = String::from_utf8(raw_value.clone())?;
            let value = if raw_str.eq_ignore_ascii_case("None") {
                Value::Null
            } else if raw_str.eq_ignore_ascii_case("T") {
                Value::Bool(true)
            } else if raw_str.eq_ignore_ascii_case("F") {
                Value::Bool(false)
            } else if let Ok(int_val) = i64::from_str(&raw_str) {
                Value::Number(int_val.into())
            } else if let Ok(float_val) = f64::from_str(&raw_str) {
                Value::Number(serde_json::Number::from_f64(float_val).unwrap())
            } else {
                Value::String(raw_str)
            };
            values.push(value);
        }

        // Insert the keyword-value pair into the reply. If there is only one value,
        // insert it directly; otherwise, insert the array of values.
        reply.keywords.insert(
            key_string,
            if values.len() == 1 {
                values.remove(0)
            } else {
                Value::Array(values)
            },
        );
    }

    // Clear temporary storage.
    key.clear();
    raw_values.clear();

    Ok(())
}

/// Processes the keywords section of a reply line and fills the `keyword` field in the [Reply] struct.
fn process_keywords(keywords: &[u8], reply: &mut Reply) -> Result<(), FromUtf8Error> {
    // Track if we are inside quotes or parsing a value (as opposed to a key).
    let mut in_double_quotes = false;
    let mut in_single_quotes = false;
    let mut in_value = false;
    let mut has_equal = false;

    // Temporary values.
    let mut key: Vec<u8> = Vec::new();
    let mut raw_values: Vec<Vec<u8>> = Vec::new(); // In case the value is a list.
    let mut raw_value: Vec<u8> = Vec::new();

    // Iterate over each character in the keywords byte slice and populate the key-value pairs.
    // In particular we need to pay attention to double quotes, semicolons (which, if outside
    // a quote indicate the keyword is over), commas (which separate elements in a list),
    // equal signs (separate keys and values, but we could have a key without a value in which case
    // there won't be an equal), and spaces (which are ignored except if inside a quote).
    for char in keywords {
        match char {
            b'=' => {
                if !in_double_quotes && !in_single_quotes && !in_value {
                    in_value = true;
                    has_equal = true;
                } else if in_value {
                    raw_value.push(*char);
                }
            }
            b'"' => {
                if !in_single_quotes {
                    in_double_quotes = !in_double_quotes;
                } else if in_value {
                    raw_value.push(*char);
                }
            }
            b'\'' => {
                if !in_double_quotes {
                    in_single_quotes = !in_single_quotes;
                } else if in_value {
                    raw_value.push(*char);
                }
            }
            b';' => {
                if !in_double_quotes && !in_single_quotes && !key.is_empty() {
                    // End of keyword. Add the current value to the list of raw values and
                    // process the keyword. Reset temporary variables.
                    raw_values.push(raw_value.clone());
                    add_to_reply_keywords(reply, &mut key, &mut raw_values, has_equal)?;
                    in_value = false;
                    has_equal = false;
                    raw_value.clear();
                } else {
                    raw_value.push(*char);
                }
            }
            b' ' => {
                if in_double_quotes || in_single_quotes {
                    raw_value.push(*char);
                }
            }
            b',' => {
                if in_double_quotes || in_single_quotes {
                    raw_value.push(*char);
                } else if in_value {
                    raw_values.push(raw_value.clone());
                    raw_value.clear();
                }
            }
            _ => {
                if in_value {
                    raw_value.push(*char);
                } else {
                    key.push(*char);
                }
            }
        }
    }

    // Handle the final keyword if any.
    if !key.is_empty() {
        raw_values.push(raw_value.clone());
        add_to_reply_keywords(reply, &mut key, &mut raw_values, has_equal)?;
    }

    Ok(())
}

/// Parses a raw reply byte slice into a [Reply] struct.
pub(crate) fn parse_reply<'a>(raw_reply: &[u8]) -> Option<Reply> {
    // Regular expression to parse the main components of the reply line.
    let line_regex = Regex::new(
        r"^(?<commander>\d+)\s+(?<commandId>\d+)\s+(?<code>[diwfe:DIWFE>])\s*(?<keywords>.+)?$",
    )
    .unwrap();

    let caps = line_regex.captures(raw_reply)?;

    // Extract commander and command ID, and initialize the Reply struct with an empty keywords map.
    let commander = String::from_utf8(caps.name("commander")?.as_bytes().to_vec()).ok()?;
    let command_id = String::from_utf8(caps.name("commandId")?.as_bytes().to_vec()).ok()?;

    let mut reply = Reply {
        commander: commander.parse::<u16>().unwrap(),
        command_id: command_id.parse::<u32>().unwrap(),
        code: caps.name("code")?.as_bytes()[0] as char,
        keywords: BTreeMap::new(),
    };

    // If there are no keywords, return the reply as is.
    let keywords_raw = match caps.name("keywords") {
        None => return Some(reply),
        Some(kws) => kws.as_bytes(),
    };

    // Process keywords.
    process_keywords(keywords_raw, &mut reply);

    Some(reply)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_reply() {
        let test_reply =
            parse_reply(b"1 20 : key1=value1; key2=42; key3=3.14; key4=T; key5=None; key6=1,F,three,\"a string; with; semicolons\"; key7=\"A string with spaces\";key8 ; key9=\"A string; with; semicolons\"; key10=\"A sentence with 'a quotation'\"").unwrap();

        assert_eq!(test_reply.commander, 1);
        assert_eq!(test_reply.command_id, 20);
        assert_eq!(test_reply.code, ':');

        assert!(!test_reply.keywords.is_empty());

        assert!(test_reply.keywords.contains_key("key1"));
        assert_eq!(
            test_reply.keywords.get("key1").unwrap(),
            &Value::String("value1".to_string())
        );

        assert!(test_reply.keywords.contains_key("key2"));
        assert_eq!(
            test_reply.keywords.get("key2").unwrap(),
            &Value::Number(42.into())
        );

        assert!(test_reply.keywords.contains_key("key3"));
        assert_eq!(
            test_reply.keywords.get("key3").unwrap(),
            &Value::Number(serde_json::Number::from_f64(3.14).unwrap())
        );

        assert!(test_reply.keywords.contains_key("key4"));
        assert_eq!(test_reply.keywords.get("key4").unwrap(), &Value::Bool(true));

        assert!(test_reply.keywords.contains_key("key5"));
        assert_eq!(test_reply.keywords.get("key5").unwrap(), &Value::Null);

        assert!(test_reply.keywords.contains_key("key6"));
        assert_eq!(
            test_reply.keywords.get("key6").unwrap(),
            &Value::Array(vec![
                Value::Number(1.into()),
                Value::Bool(false),
                Value::String("three".to_string()),
                Value::String("a string; with; semicolons".to_string())
            ])
        );

        assert!(test_reply.keywords.contains_key("key7"));
        assert_eq!(
            test_reply.keywords.get("key7").unwrap(),
            &Value::String("A string with spaces".to_string())
        );

        assert!(test_reply.keywords.contains_key("key8"));
        assert_eq!(test_reply.keywords.get("key8").unwrap(), &Value::Null);

        assert!(test_reply.keywords.contains_key("key9"));
        assert_eq!(
            test_reply.keywords.get("key9").unwrap(),
            &Value::String("A string; with; semicolons".to_string())
        );

        assert!(test_reply.keywords.contains_key("key10"));
        assert_eq!(
            test_reply.keywords.get("key10").unwrap(),
            &Value::String("A sentence with 'a quotation'".to_string())
        );
    }
}
