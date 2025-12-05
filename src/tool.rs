/*
 *  @Author: José Sánchez-Gallego (gallegoj@uw.edu)
 *  @Date: 2025-12-04
 *  @Filename: tool.rs
 *  @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)
 */

use std::collections::BTreeSet;

/// A structure to manage command IDs and their associated UUIDs.
/// Command IDs are allocated from a pool of 0-255.
/// When a command is finished, its ID is returned to the pool.
pub struct CommandID {
    /// A HashMap mapping command IDs to UUID strings.
    pub command_id_to_uuid: std::collections::HashMap<u16, String>,
    /// A HashMap mapping UUID strings to commander names.
    pub uuid_to_commander: std::collections::HashMap<String, String>,
    /// A BTreeSet of available command IDs.
    pub command_ids: BTreeSet<u16>,
}

impl CommandID {
    /// Creates a new CommandID manager with all command IDs available.
    pub fn new() -> Self {
        Self {
            command_id_to_uuid: std::collections::HashMap::new(),
            uuid_to_commander: std::collections::HashMap::new(),
            command_ids: (1..65535).rev().collect(),
        }
    }

    /// Registers a command with a given UUID, commander name, and command ID.
    pub fn register_command(&mut self, uuid: &str, commander: &str, command_id: u16) {
        self.command_id_to_uuid.insert(command_id, uuid.to_string());
        self.uuid_to_commander
            .insert(uuid.to_string(), commander.to_string());
    }

    /// Retrieves and removes an available command ID from the pool.
    pub fn get_command_id(&mut self) -> u16 {
        let elt = self.command_ids.iter().next().cloned().unwrap();
        self.command_ids.take(&elt).unwrap()
    }

    /// Finishes a command by its ID, removing its associations and returning the UUID.
    /// The command ID is returned to the pool of available IDs.
    pub fn finish_command(&mut self, command_id: u16) -> Option<String> {
        let uuid = self.command_id_to_uuid.remove(&command_id);

        if !self.command_ids.contains(&command_id) {
            self.command_ids.insert(command_id);
        }

        if let Some(uuid) = uuid {
            self.uuid_to_commander.remove(uuid.as_str());
            return Some(uuid);
        }

        return None;
    }

    /// Retrieves the UUID associated with a given command ID, if it exists.
    pub fn get_uuid(&self, command_id: u16) -> Option<&String> {
        self.command_id_to_uuid.get(&command_id)
    }

    /// Checks if a command ID is currently in use.
    pub fn is_command_id_in_use(&self, command_id: u16) -> bool {
        self.command_id_to_uuid.contains_key(&command_id)
    }

    /// Retrieves the commander name associated with a given UUID, if it exists.
    pub fn get_commander(&self, uuid: &str) -> Option<&String> {
        self.uuid_to_commander.get(uuid)
    }
}

#[cfg(test)]
mod tests {
    use super::CommandID;

    #[test]
    fn test_command_id_init() {
        let cmd_id = CommandID::new();
        assert_eq!(cmd_id.command_ids.len(), 65534);
        assert_eq!(cmd_id.command_id_to_uuid.len(), 0);
        assert_eq!(cmd_id.uuid_to_commander.len(), 0);
    }

    #[test]
    fn test_command_id_get_command_id() {
        let mut cmd_id = CommandID::new();
        let id = cmd_id.get_command_id();
        assert_eq!(id, 1);

        let id2 = cmd_id.get_command_id();
        assert_eq!(id2, 2);

        cmd_id.finish_command(id);
        let id3 = cmd_id.get_command_id();
        assert_eq!(id3, 1);
    }

    #[test]
    fn test_command_id_register() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        let mut cmd_id = CommandID::new();
        let id = cmd_id.get_command_id();
        cmd_id.register_command(uuid, "commander1", id);

        assert!(cmd_id.is_command_id_in_use(id));
        assert_eq!(cmd_id.get_uuid(id).unwrap(), uuid);
        assert_eq!(cmd_id.command_id_to_uuid.len(), 1);
        assert_eq!(cmd_id.uuid_to_commander.len(), 1);
        assert_eq!(cmd_id.is_command_id_in_use(id), true);
        assert_eq!(cmd_id.get_commander(uuid).unwrap(), "commander1");
    }

    #[test]
    fn test_command_id_finish() {
        let uuid = "123e4567-e89b-12d3-a456-426614174000";
        let mut cmd_id = CommandID::new();
        let id = cmd_id.get_command_id();
        cmd_id.register_command(uuid, "commander1", id);

        let finished_uuid = cmd_id.finish_command(id).unwrap();
        assert_eq!(finished_uuid, uuid);
        assert!(!cmd_id.is_command_id_in_use(id));
        assert!(cmd_id.get_uuid(id).is_none());
        assert!(cmd_id.get_commander(uuid).is_none());
    }
}
