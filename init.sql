CREATE TABLE IF NOT EXISTS public.orders (
    order_id BIGINT PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    amount NUMERIC(12, 3) NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.orders REPLICA IDENTITY FULL;

INSERT INTO public.orders (order_id, customer_name, amount, status)
VALUES (1, 'Seed Customer', 25.500, 'CREATED')
ON CONFLICT (order_id) DO NOTHING;