use blinc_app::prelude::*;

use crate::render::context::RenderContext;
use crate::render::interpreter::ResolvedArgs;

pub fn build(args: &ResolvedArgs, _ctx: &RenderContext) -> Div {
    let w = args.get_f64("width").or(args.get_f64("w")).unwrap_or(0.0) as f32;
    let h = args.get_f64("height").or(args.get_f64("h")).unwrap_or(0.0) as f32;

    let mut el = div();
    if w > 0.0 {
        el = el.w(w);
    }
    if h > 0.0 {
        el = el.h(h);
    }
    if w == 0.0 && h == 0.0 {
        el = el.flex_1();
    }
    el
}
