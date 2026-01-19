--
-- PostgreSQL schema for sample-app
-- This is the schema file that sql-check will validate queries against.
-- Generated to match what pg_dump --schema-only would produce.
--

--
-- Name: users; Type: TABLE
--

CREATE TABLE users (
    id uuid NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    metadata jsonb NOT NULL DEFAULT '{}',
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT users_pkey PRIMARY KEY (id),
    CONSTRAINT users_email_key UNIQUE (email)
);

--
-- Name: profiles; Type: TABLE
--

CREATE TABLE profiles (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    bio text,
    avatar_url text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT profiles_pkey PRIMARY KEY (id),
    CONSTRAINT profiles_user_id_fkey FOREIGN KEY (user_id)
        REFERENCES users(id) ON DELETE CASCADE,
    CONSTRAINT profiles_user_id_key UNIQUE (user_id)
);

--
-- Name: idx_users_email; Type: INDEX
--

CREATE INDEX idx_users_email ON users USING btree (email);

--
-- Name: idx_profiles_user_id; Type: INDEX
--

CREATE INDEX idx_profiles_user_id ON profiles USING btree (user_id);
