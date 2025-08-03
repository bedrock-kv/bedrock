# Documentation-Code Alignment Checklist

Use this checklist when reviewing and improving documentation sections to ensure accuracy, clarity, and alignment between documentation and implementation.

## 1. Pre-Analysis

- [ ] Read the documentation section completely
- [ ] Read the corresponding source file(s) and moduledoc(s)
- [ ] Identify the primary function(s) being documented

## 2. Accuracy Verification

- [ ] Compare documented behavior against actual source code implementation
- [ ] Verify function signatures, parameters, and return types match descriptions
- [ ] Check that timeout values and configuration details are current
- [ ] Ensure error handling and failure modes are accurately described
- [ ] Validate that all code paths mentioned in docs actually exist
- [ ] **Verify algorithmic correctness** - Ensure complex algorithms match architectural requirements (e.g., shard-aware calculations, multi-level minimums)

## 3. Precision & Clarity Improvements

- [ ] Replace vague or ambiguous language with specific behavioral descriptions
- [ ] Clarify causation vs correlation in logical flows
- [ ] Add missing critical implementation details that affect behavior
- [ ] Fix any logical errors or misconceptions about how the system works
- [ ] Ensure technical terms are used consistently and correctly

## 4. Educational Narrative Style

- [ ] **Problem-first approach** - Start by explaining what problem the phase solves and why it's necessary
- [ ] **Logical flow without artificial breaks** - Write as single flowing narrative rather than choppy subsections
- [ ] **Explain constraints that drive design** - Make clear why different approaches are taken (e.g., logs vs storage)
- [ ] **Use concrete examples** - Provide specific scenarios that make abstract concepts tangible
- [ ] **Explain causation clearly** - Connect "why" decisions are made to "what" the implementation does
- [ ] **Accessible analogies** - Use relatable metaphors (e.g., "job postings" for vacancies) when helpful
- [ ] **Right level of detail** - Focus on key concepts and outcomes rather than exhaustive technical minutiae
- [ ] **Benefits flow naturally** - Integrate advantages into the narrative rather than separating into "benefits" sections

## 5. Structure & Presentation

- [ ] Convert abstract Input/Output descriptions to specific field/parameter lists matching other sections
- [ ] Use bullet points or lists for better readability of multiple items
- [ ] Remove redundant sections where information is already covered in prose
- [ ] Fix any markdown formatting issues
- [ ] Ensure proper cross-references and links
- [ ] **Question precision of input descriptions** - Verify what data is actually used vs. what's merely passed through

## 6. Source Code Updates

- [ ] Update any outdated configuration values or constants
- [ ] Implement missing functions referenced in documentation
- [ ] Apply architectural improvements identified during analysis
- [ ] Ensure return types match documented behavior

## 7. Moduledoc Refinement

- [ ] Rewrite moduledoc to be concise and factual
- [ ] Include key behavioral details that affect usage
- [ ] Add cross-references to detailed narrative documentation
- [ ] Remove vague language and replace with precise descriptions
- [ ] Ensure moduledoc matches narrative documentation terminology
- [ ] **Match educational tone** - Use same problem-solving and planning language as narrative

## 8. Testing & Validation

- [ ] Add tests for any new behavioral paths or edge cases
- [ ] Run existing tests to ensure no regressions
- [ ] Add missing test coverage for documented scenarios

## 9. Final Verification

- [ ] Re-read documentation section against updated source code
- [ ] Verify input/output specifications match actual implementation
- [ ] Confirm all behavioral descriptions align with code
- [ ] **Check narrative flow** - Ensure section reads as cohesive educational prose without redundancy
- [ ] **Validate information order** - Problem → constraints → strategies → outcomes

---

This checklist can be applied to any documentation section to ensure accuracy, clarity, and alignment between documentation and implementation.