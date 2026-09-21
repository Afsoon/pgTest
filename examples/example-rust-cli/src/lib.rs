use tokio_postgres::{Client, Error};

#[derive(Debug, PartialEq, Eq)]
pub struct User {
    pub id: i32,
    pub name: String,
}

pub async fn create_user(client: &Client, id: i32, name: &str) -> Result<User, Error> {
    let row = client
        .query_one("INSERT INTO users (id, name) VALUES ($1, $2) RETURNING id, name", &[&id, &name])
        .await?;
    Ok(User { id: row.get(0), name: row.get(1) })
}

pub async fn list_users(client: &Client) -> Result<Vec<User>, Error> {
    let rows = client.query("SELECT id, name FROM users ORDER BY id", &[]).await?;
    Ok(rows.into_iter().map(|row| User { id: row.get(0), name: row.get(1) }).collect())
}
