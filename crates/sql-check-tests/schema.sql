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

--
-- Name: categories; Type: TABLE
--

CREATE TABLE categories (
    id uuid NOT NULL,
    name text NOT NULL,
    parent_id uuid,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT categories_pkey PRIMARY KEY (id),
    CONSTRAINT categories_parent_fkey FOREIGN KEY (parent_id)
        REFERENCES categories(id) ON DELETE SET NULL
);

--
-- Name: products; Type: TABLE
--

CREATE TABLE products (
    id uuid NOT NULL,
    name text NOT NULL,
    description text,
    price numeric(10,2) NOT NULL,
    category_id uuid,
    stock_quantity integer NOT NULL DEFAULT 0,
    is_active boolean NOT NULL DEFAULT true,
    tags text[],
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT products_pkey PRIMARY KEY (id),
    CONSTRAINT products_category_fkey FOREIGN KEY (category_id)
        REFERENCES categories(id) ON DELETE SET NULL
);

--
-- Name: orders; Type: TABLE
--

CREATE TABLE orders (
    id uuid NOT NULL,
    user_id uuid NOT NULL,
    status text NOT NULL DEFAULT 'pending',
    total_amount numeric(10,2) NOT NULL DEFAULT 0,
    notes text,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT orders_pkey PRIMARY KEY (id),
    CONSTRAINT orders_user_fkey FOREIGN KEY (user_id)
        REFERENCES users(id) ON DELETE CASCADE
);

--
-- Name: order_items; Type: TABLE
--

CREATE TABLE order_items (
    id uuid NOT NULL,
    order_id uuid NOT NULL,
    product_id uuid NOT NULL,
    quantity integer NOT NULL,
    unit_price numeric(10,2) NOT NULL,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT order_items_pkey PRIMARY KEY (id),
    CONSTRAINT order_items_order_fkey FOREIGN KEY (order_id)
        REFERENCES orders(id) ON DELETE CASCADE,
    CONSTRAINT order_items_product_fkey FOREIGN KEY (product_id)
        REFERENCES products(id) ON DELETE RESTRICT
);

--
-- Name: idx_products_category; Type: INDEX
--

CREATE INDEX idx_products_category ON products USING btree (category_id);

--
-- Name: idx_orders_user; Type: INDEX
--

CREATE INDEX idx_orders_user ON orders USING btree (user_id);

--
-- Name: idx_order_items_order; Type: INDEX
--

CREATE INDEX idx_order_items_order ON order_items USING btree (order_id);
