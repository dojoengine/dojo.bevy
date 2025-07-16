//! Basic example of Dojo plugin usage.
//!
//! TODO:
//! 1. Add text to explain bindings.
//! 2. Add the player address on the top of the cube.
//! 3. Choose a random color for every new spawned cube.
//! 4. Add the delete entity concept, when pressing backspace on a cube.
//!
//! Switching between accounts will show the different cubes moving.

use bevy::input::ButtonState;
use bevy::{input::keyboard::KeyboardInput, prelude::*};
use dojo_types::schema::Struct;
use starknet::core::types::Call;
use starknet::core::types::Felt;
use starknet::macros::selector;
use std::collections::HashSet;
use torii_grpc_client::types::{Pagination, PaginationDirection, Query as ToriiQuery};

use dojo_bevy_plugin::{
    DojoEntityUpdated, DojoInitializedEvent, DojoPlugin, DojoResource, TokioRuntime,
};

const TORII_URL: &str = "http://localhost:8080";
const KATANA_URL: &str = "http://0.0.0.0:5050";

// Manifest related constants.
// This is hard coded for now, but we should implement a logic to load
// those data from the Dojo manifest.
const WORLD_ADDRESS: Felt =
    Felt::from_hex_unchecked("0x07cb61df9ec4bdd30ca1f195bc20ff3c7afd0e45e3a3f156767fe05129fd499b");
const ACTION_ADDRESS: Felt =
    Felt::from_hex_unchecked("0x0693bc04141539bb8608db41662f7512b57e116087c1c7a529eca0ed4c774ad5");
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
        .add_plugins(DojoPlugin)
        .init_resource::<DojoResource>()
        .init_resource::<TokioRuntime>()
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
    tokio: Res<TokioRuntime>,
    mut dojo: ResMut<DojoResource>,
    mut keyboard_input_events: EventReader<KeyboardInput>,
) {
    for event in keyboard_input_events.read() {
        let key_code = event.key_code;
        let is_pressed = event.state == ButtonState::Pressed;

        match key_code {
            KeyCode::KeyC if is_pressed => {
                // Dojo connect uses the dojo system to check for async tasks
                // that initializes connections to Torii and Starknet account.
                dojo.connect_torii(&tokio, TORII_URL.to_string(), WORLD_ADDRESS);
                dojo.connect_predeployed_account(&tokio, KATANA_URL.to_string(), 0);

                // Hence, when here, we are not yet connected, until the next
                // frame.
                // TODO: this could be improved or by using a `is_ready()` function.
            }
            KeyCode::Space if is_pressed => {
                info!("Spawning.");
                let calls = vec![Call {
                    to: ACTION_ADDRESS,
                    selector: SPAWN_SELECTOR,
                    calldata: vec![],
                }];
                dojo.queue_tx(&tokio, calls);
            }
            KeyCode::KeyS if is_pressed => {
                info!("Setting up Torii subscription.");
                dojo.subscribe_entities(&tokio, "position".to_string(), None);
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

                dojo.queue_tx(&tokio, calls);
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
                MeshMaterial3d(materials.add(Color::srgb(0.8, 0.2, 0.2))),
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

/// Reacts on Dojo events, which are emitted by the Dojo plugin.
///
/// Any `queue_retrieve_entities` or `subscribe_entities` call will trigger
/// the `DojoEntityUpdated` event.
fn on_dojo_events(
    mut dojo: ResMut<DojoResource>,
    tokio: Res<TokioRuntime>,
    mut ev_initialized: EventReader<DojoInitializedEvent>,
    mut ev_retrieve_entities: EventReader<DojoEntityUpdated>,
    mut ev_position_updated: EventWriter<PositionUpdatedEvent>,
) {
    for _ in ev_initialized.read() {
        info!("Dojo initialized.");

        // Initial fetch, which will make the Dojo plugin to send
        // the query Torii, and trigger the `DojoEntityUpdated` event.
        dojo.queue_retrieve_entities(
            &tokio,
            ToriiQuery {
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
            },
        );
    }

    // Since the deserialization of the models is project specific,
    // currently the way it is done is by emitting an event for each
    // models updates we are interested in.
    // This may become too much for a large number of models though.
    // Maybe the solution would be to generate a plugin via bindgen,
    // that registers all of this automatically.
    for ev in ev_retrieve_entities.read() {
        info!(entity_id = ?ev.entity_id, "Torii update");

        // Felt::ZERO is being emitted once, when the subscription is initialized.
        // We don't want to spawn a cube for this.
        if ev.entity_id == Felt::ZERO {
            continue;
        }

        for m in &ev.models {
            debug!("model: {:?}", &m);

            match m.name.as_str() {
                "di-Position" => {
                    ev_position_updated.write(PositionUpdatedEvent(m.into()));
                }
                "di-Moves" => {}
                _ => {
                    warn!("Model not handled: {:?}", m);
                }
            };
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

/// This implementation shows a manual way to map data from the Position model in Cairo.
/// Ideally, we want a binding generation to do that for us.
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
