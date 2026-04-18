pub mod ex1_1;
pub mod ex1_2;
pub mod ex1_3;
pub mod ex1_executor;
pub mod ex2_1;
pub mod ex2_2;
pub mod ex3;
pub mod solutions;

fn main() {
    let arg = std::env::args().nth(1);

    match arg.as_deref() {
        Some("1.1") => ex1_1::run(),
        Some("1.2") => ex1_2::run(),
        Some("1.3") => ex1_3::run(),
        Some("2.1") => ex2_1::run(),
        Some("2.2") => ex2_2::run(),
        Some("3") => ex3::run(),
        _ => print_usage(),
    }
}

fn print_usage() {
    println!("Usage: workshop <exercise>");
    println!();
    println!("  Exercise 1 — Implement Futures:");
    println!("    1.1 — YieldNow");
    println!("    1.2 — Delay");
    println!("    1.3 — Oneshot");
    println!();
    println!("  Exercise 2 — Build an Executor:");
    println!("    2.1 — block_on (drive a single future)");
    println!("    2.2 — spawn (multi-task executor)");
    println!();
    println!("  Exercise 3 — Mini Reactor:");
    println!("    3   — echo server (mio reactor + executor)");
}
