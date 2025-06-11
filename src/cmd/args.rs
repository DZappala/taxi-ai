use clap::Parser;

#[derive(Parser, Debug)]
pub struct Args {
    #[arg(short('b'), long)]
    rebuild: bool,
}

impl Args {
    pub fn rebuild(&self) -> bool {
        self.rebuild
    }
}
