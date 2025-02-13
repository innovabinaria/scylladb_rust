use anyhow::{Context, Result};
use futures::TryStreamExt;
use scylla::{Session, SessionBuilder};
use std::error::Error;
use uuid::Uuid;

const DB_URL: &str = "127.0.0.1:9042"; // 📌 Dirección de ScyllaDB Local
const KEYSPACE: &str = "my_keyspace";
const TABLE: &str = "users";

// Módulo para manejar operaciones de la base de datos
mod db_operations {
    use super::*;

    // Conectar a ScyllaDB
    pub async fn connect_to_scylla() -> Result<Session, Box<dyn Error>> {
        let session = SessionBuilder::new()
            .known_node(DB_URL) // Nodo local para pruebas
            .build()
            .await?;
        println!("✅ Conectado a ScyllaDB exitosamente.");
        Ok(session)
    }

    // Insertar un nuevo usuario en la tabla `users`
    pub async fn insert_user(
        session: &Session,
        _id: Uuid,
        age: i32,
        name: &str,
        email: &str,
    ) -> Result<(), Box<dyn Error>> {
        let id = Uuid::new_v4(); // Genera un nuevo UUID
        let query = format!(
            "INSERT INTO {}.{} (id, age, name, email) VALUES (?, ?, ?, ?)",
            KEYSPACE, TABLE
        );

        session
            .query_unpaged(query, (_id, age, name, email))
            .await
            .context("🚨 Error al insertar usuario")?;

        println!(
            "✅ Usuario insertado con éxito: ID={}, Edad={}, Nombre={}, Email={}",
            id, age, name, email
        );
        Ok(())
    }

    // Listar todos los usuarios de la tabla `users`
    pub async fn list_users(session: &Session) -> Result<(), Box<dyn Error>> {
        // Ejecutar la consulta y obtener resultados como stream
        let query = format!("SELECT id, name, email FROM {}.{}", KEYSPACE, TABLE);

        let mut iter = session
            .query_iter(query, &[])
            .await?
            .rows_stream::<(Uuid, String, String)>()?; // ⬅️ Usa String en lugar de &str

        // Iterar sobre los resultados
        println!("\n📋 Lista de Usuarios:");
        while let Some((id, name, email)) = iter.try_next().await? {
            println!("📌 Usuario: ID={}, Nombre={}, Email={}", id, name, email);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Conectar a ScyllaDB
    let session = db_operations::connect_to_scylla().await?;

    // Insertar un nuevo usuario
    let new_user_id = Uuid::new_v4();
    db_operations::insert_user(
        &session,
        new_user_id,
        45,
        "Victor Aguayo",
        "victora@example.com",
    )
    .await?;

    // Listar todos los usuarios
    db_operations::list_users(&session).await?;

    Ok(())
}
