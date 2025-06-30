//! Basic example of Dojo v2 plugin usage using native Bevy tasks.
//!
//! This example demonstrates the same functionality as intro.rs but using
//! the new v2 plugin with native Bevy task integration.

use bevy::input::ButtonState;
use bevy::{input::keyboard::KeyboardInput, prelude::*};
use dojo_types::schema::Struct;
use starknet::core::types::Call;
use starknet::core::types::Felt;
use starknet::macros::selector;
use std::collections::HashSet;
use torii_grpc_client::types::{Pagination, PaginationDirection, Query as ToriiQuery};

use dojo_bevy_plugin::{DojoEntityUpdatedV2, DojoInitializedEventV2, DojoPluginV2, DojoResourceV2};

const TORII_URL: &str = "http://localhost:8080";
const KATANA_URL: &str = "http://0.0.0.0:5050";

// Manifest related constants.
const WORLD_ADDRESS: Felt =
    Felt::from_hex_unchecked("0x04d9778a74d2c9e6e7e4a24cbe913998a80de217c66ee173a604d06dea5469c3");
const ACTION_ADDRESS: Felt =
    Felt::from_hex_unchecked("0x00b056c9813fdc442118bdfead6fda526e5daa5fd7d543304117ed80154ea752");
const SPAWN_SELECTOR: Felt = selector!("spawn");
const MOVE_SELECTOR: Felt = selector!("move");

/// This event will be triggered every time the position is updated.
#[derive(Event)]
struct PositionUpdatedEvent(pub Position);

/// A very simple cube to represent the player.
#[derive(Component)]
pub struct Cube {
    pub player: Felt,
}

#[derive(Resource, Default)]
struct EntityTracker {
    existing_entities: HashSet<Felt>,
}

/// Main entry point.
fn main() {
    App::new()
        .add_plugins(DefaultPlugins)
        .add_plugins(DojoPluginV2) // Use the v2 plugin
        .init_resource::<DojoResourceV2>() // Use v2 resource
        .init_resource::<EntityTracker>()
        .add_event::<PositionUpdatedEvent>()
        .add_systems(Startup, setup)
        .add_systems(
            Update,
            (
                handle_keyboard_input,
                on_dojo_events,
                (update_cube_position).after(on_dojo_events),
            ),
        )
        .run();
}

/// This system is responsible for handling the keyboard input.
fn handle_keyboard_input(
    mut dojo: ResMut<DojoResourceV2>, // Use v2 resource
    mut keyboard_input_events: EventReader<KeyboardInput>,
) {
    for event in keyboard_input_events.read() {
        let key_code = event.key_code;
        let is_pressed = event.state == ButtonState::Pressed;

        match key_code {
            KeyCode::KeyC if is_pressed => {
                // Connect using v2 methods (no tokio runtime needed)
                dojo.connect_torii(TORII_URL.to_string(), WORLD_ADDRESS);
                dojo.connect_predeployed_account(KATANA_URL.to_string(), 0);
            }
            KeyCode::Space if is_pressed => {
                info!("Spawning (v2).");
                let calls = vec![Call {
                    to: ACTION_ADDRESS,
                    selector: SPAWN_SELECTOR,
                    calldata: vec![],
                }];
                dojo.queue_tx(calls); // No tokio runtime needed
            }
            KeyCode::KeyS if is_pressed => {
                info!("Setting up Torii subscription (v2).");
                dojo.subscribe_entities("position".to_string(), None);
            }
            KeyCode::ArrowLeft | KeyCode::ArrowRight | KeyCode::ArrowUp | KeyCode::ArrowDown
                if is_pressed =>
            {
                let direction = match key_code {
                    KeyCode::ArrowLeft => 0,
                    KeyCode::ArrowRight => 1,
                    KeyCode::ArrowUp => 2,
                    KeyCode::ArrowDown => 3,
                    _ => panic!("Invalid key code"),
                };

                let calls = vec![Call {
                    to: ACTION_ADDRESS,
                    selector: MOVE_SELECTOR,
                    calldata: vec![Felt::from(direction)],
                }];

                dojo.queue_tx(calls); // No tokio runtime needed
            }
            _ => continue,
        }
    }
}

/// Updates the cube position by reacting to the dedicated event
/// for new position updates.
fn update_cube_position(
    mut commands: Commands,
    mut meshes: ResMut<Assets<Mesh>>,
    mut materials: ResMut<Assets<StandardMaterial>>,
    mut entity_tracker: ResMut<EntityTracker>,
    mut ev_position_updated: EventReader<PositionUpdatedEvent>,
    mut query: Query<(&mut Transform, &Cube)>,
) {
    for ev in ev_position_updated.read() {
        let Position { x, y, player } = ev.0;

        if !entity_tracker.existing_entities.contains(&player) {
            commands.spawn((
                Mesh3d(meshes.add(Cuboid::new(0.5, 0.5, 0.5))),
                MeshMaterial3d(materials.add(Color::srgb(0.8, 0.7, 0.2))), // Different color for v2
                Cube { player },
                Transform::from_xyz(x as f32, y as f32, 0.0),
            ));

            entity_tracker.existing_entities.insert(player);
        } else {
            for (mut transform, cube) in query.iter_mut() {
                if cube.player == player {
                    transform.translation = Vec3::new(x as f32, y as f32, 0.0);
                }
            }
        }
    }
}

/// Reacts on Dojo v2 events.
fn on_dojo_events(
    mut dojo: ResMut<DojoResourceV2>,
    mut ev_initialized: EventReader<DojoInitializedEventV2>, // Use v2 events
    mut ev_retrieve_entities: EventReader<DojoEntityUpdatedV2>, // Use v2 events
    mut ev_position_updated: EventWriter<PositionUpdatedEvent>,
) {
    for _ in ev_initialized.read() {
        info!("Dojo v2 initialized.");

        // Initial fetch using v2 resource
        dojo.queue_retrieve_entities(ToriiQuery {
            clause: None,
            pagination: Pagination {
                limit: 100,
                cursor: None,
                direction: PaginationDirection::Forward,
                order_by: vec![],
            },
            no_hashed_keys: false,
            models: vec![],
            historical: false,
        });
    }

    for ev in ev_retrieve_entities.read() {
        info!(entity_id = ?ev.entity_id, "Torii v2 update");

        if ev.entity_id == Felt::ZERO {
            continue;
        }

        for m in &ev.models {
            debug!("model: {:?}", &m);

            match m.name.as_str() {
                "di-Position" => {
                    ev_position_updated.write(PositionUpdatedEvent(m.into()));
                }
                name if name == "di-Moves".to_string() => {}
                _ => {
                    warn!("Model not handled: {:?}", m);
                }
            }
        }
    }
}

/// The position of the player in the game.
#[derive(Component, Debug)]
pub struct Position {
    pub player: Felt,
    pub x: u32,
    pub y: u32,
}

/// Manual conversion from Dojo struct to Position.
impl From<&Struct> for Position {
    fn from(struct_value: &Struct) -> Self {
        let player = struct_value
            .get("player")
            .unwrap()
            .as_primitive()
            .unwrap()
            .as_contract_address()
            .unwrap();
        let x = struct_value
            .get("x")
            .unwrap()
            .as_primitive()
            .unwrap()
            .as_u32()
            .unwrap();
        let y = struct_value
            .get("y")
            .unwrap()
            .as_primitive()
            .unwrap()
            .as_u32()
            .unwrap();

        Position { player, x, y }
    }
}

/// Setups the scene with basic light.
pub fn setup(mut commands: Commands) {
    commands.spawn((
        DirectionalLight::default(),
        Transform::from_xyz(0.0, 0.0, 30.0).looking_at(Vec3::ZERO, Vec3::Y),
    ));
    commands.spawn((
        Camera3d::default(),
        Transform::from_xyz(0.0, 0.0, 30.0).looking_at(Vec3::ZERO, Vec3::Y),
    ));
}
